{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-|
Module      : Network.NSQ.Connection
Description : Protocol/parser layer for the NSQ client library.

This is the low level client connection to the nsqd. It is recommended to use the
higher level library when those come out.
-}
module Network.NSQ.Connection
    ( defaultConfig
    , establish

    ) where

import           Control.Concurrent.Async.Lifted (race_)
import           Control.Concurrent.STM
import           Control.Monad.Catch (MonadMask)
import           Control.Monad.Logger
import           Control.Monad.State.Strict
import           Control.Monad.Trans.Control (MonadBaseControl)
import qualified Data.ByteString as BS
import           Data.Text (Text)
import qualified Data.Text as T
import           Formatting
import           Network.HostName
import           Pipes
import qualified Pipes.Attoparsec as PA
import qualified Pipes.Network.TCP as PNT
import qualified Pipes.Prelude as PP
import           Prelude hiding (log)

import qualified Network.NSQ.Identify as NSQ
import qualified Network.NSQ.Parser as NSQ
import           Network.NSQ.Types (NSQConnection(..), Message, Command, LogName
                                   , server, port, logName, identConf)
import qualified Network.NSQ.Types as NSQ


-- TODO: standardize on some sort of logger hierchary (nsq server/topic?)
-- NSQ.[subsystem].[topic].[connection] - message
-- NSQ.[subsystem].[custom] ....
--
-- | Attempt to come up with an intelligent default 'NSQConnection' default
-- by discovering your client's hostname and reusing it for the client id
-- for the 'IdentifyMetadata'
defaultConfig :: String -> IO NSQConnection
defaultConfig serverHost = do
    localHost <- T.pack <$> getHostName
    let localClientId = T.takeWhile (/= '.') localHost
    let localIdent = NSQ.defaultIdentify localClientId localHost

    return $ NSQConnection serverHost 4150 "NSQ.Connection." localIdent

-- | Establish a session with the specified nsqd using the provided
-- 'TQueue' so that the actual data processing can be done in a decoupled
-- manner from the hsnsq stack.
--
-- This supports connecting to a specific nsqd, it is however recommended
-- in the future when the feature comes out to user a higher layer that
-- handles the load balancing between multiple nsqd.
establish :: (MonadMask m, MonadBaseControl IO m, MonadIO m, MonadLogger m) =>
             NSQConnection -> TQueue Message -> TQueue Command -> m ()
establish conn topicQueue reply =
  -- Establish stocket
  PNT.connect (server conn) (show $ port conn) $ \(sock, _) -> do

  -- TODO: maybe consider PNT.fromSocketN so that we can adjust fetch size if needed downstream
  let send = (log "send" $ logName conn) >-> PNT.toSocket sock
      recv = PNT.fromSocket sock 8192 >-> (log "recv" $ logName conn)

  -- Establish NSQ first then go into normal handle mode
  establishNSQ conn recv send
  race_
    (handleNSQ conn recv send topicQueue)
    (runEffect $ handleReply reply >-> showCommand >-> send) -- Handles user replies

-- | Pump a 'Command' into the network from the 'TQueue'
handleReply :: (MonadIO m, MonadLogger m) => TQueue Command -> Producer NSQ.Command m ()
handleReply queue = forever $ do
    cmd <- liftIO $ atomically $ readTQueue queue
    yield cmd

-- | Handles the parsing, error reporting, and logging of the NSQ
-- connection, then eventually pumping the 'Message' into the 'TQueue'
-- for the consumer to process.
handleNSQ :: (MonadIO m, MonadLogger m) => NSQConnection -> Producer BS.ByteString m () -> Consumer BS.ByteString m () -> TQueue Message -> m ()
handleNSQ sc recv send topicQueue = do
    runEffect $ (nsqParserErrorLogging (logName sc) recv) >-> (command (logName sc) topicQueue) >-> showCommand >-> send
    return ()

-- | Establish the connection to the nsqd and run the initial handshake
-- upto completion then hand it off to the 'handleNSQ' for handling the
-- regular protocol/message.
establishNSQ :: (MonadIO m, MonadLogger m) => NSQConnection -> Producer BS.ByteString m () -> Consumer BS.ByteString m () -> m ()
establishNSQ sc recv send = do
    -- Initial handshake to kick off the handshake
    runEffect $ (initialHandshake $ identConf sc) >-> showCommand >-> send

    -- Rest of the handshake process (parsing and dealing with identification)
    runEffect $ (nsqParserErrorLogging (logName sc) recv) >-> identReply

    return ()

    where
        -- Initial Handshake
        initialHandshake im = do
            yield $ NSQ.Protocol
            yield $ NSQ.Identify im
            return ()

        -- Process the ident reply
        identReply = do
            identR <- await

            -- TODO: do stuff with it
            lift $ $logDebugS (logName sc) $ sformat ("IDENT: " % shown ) identR

            return ()

-- | Parses incoming nsq messages and emits any errors to a log and keep going
nsqParserErrorLogging :: (MonadIO m, MonadLogger m) => LogName -> Producer BS.ByteString m () -> Producer NSQ.Message m ()
nsqParserErrorLogging l producer = do
    (result, rest) <- lift $ runStateT (PA.parse NSQ.message) producer

    case result of
        Nothing -> lift $ $logErrorS l "Pipe is exhausted for nsq parser"
        Just y  -> do
            case y of
                Right x -> do
                  lift $ $logDebugS l (sformat ("msg: " % shown) x)
                  yield x
                Left x  -> lift $ $logErrorS l (T.pack $ show x)
            nsqParserErrorLogging l rest

-- | Format outbound NSQ Commands
showCommand :: Monad m => Pipe NSQ.Command BS.ByteString m ()
showCommand = PP.map NSQ.encode

-- | Log anything that passes through this stream to a logfile
log :: (MonadLogger m) => Text -> LogName -> Pipe BS.ByteString BS.ByteString m r
log w l = forever $ do
    x <- await
    lift $ $logDebugS l $ sformat (stext % ": " % shown) w x -- TODO: need a better way to log raw protocol messages
    yield x

-- | Generic processor for processing various messages automatically such
-- as 'Heartbeat' and just passing upstream (to 'TQueue') the actual
-- 'Message' that needs processing.
command :: (MonadIO m, MonadLogger m) =>
           LogName -> TQueue Message -> Pipe NSQ.Message NSQ.Command m ()
command l topicQueue = forever $ do
  msg <- await

  case msg of
    -- TODO: currently no-op
    NSQ.OK                  ->
      return ()
    NSQ.Heartbeat           ->
      yield $ NSQ.NOP
    -- TODO: Implement a way to close our connection gracefully
    NSQ.CloseWait           ->
      lift $ $logInfoS l ("Error: Server closed queue")
    -- TODO: should pass it onto the client or have a callback
    NSQ.Error e             ->
      lift $ $logErrorS l $ sformat ("Error: " % shown) e
    NSQ.Message _ _ _ _     ->
      liftIO $ atomically $ writeTQueue topicQueue msg
    -- TODO: should pass it onto the client or have a callback
    NSQ.CatchAllMessage f m ->
      lift $ $logWarnS l $ sformat ("Error: Frame - " % shown % " - Msg - " % shown) f m
