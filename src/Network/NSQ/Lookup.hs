{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoImplicitPrelude #-}
-- | API to <http://nsq.io/components/nsqlookupd.html NSQLOOKUPD>
-- 
-- @
--    let nsqlookupUrl = "http://192.168.0.33:4161"::Text
--    nsqlookupExec nsqlookupUrl $ channels "clients"
-- @
--
-- > Right ["requests"]
module Network.NSQ.Lookup
       ( -- * NSQLOOKUPD API
         -- ** API Monad
         NsqLookup
       , nsqlookupExec
       , nsqlookupRun
         -- ** Return data
       , Error(..)
       , Producer(..)
       , Lookup(..)
       , OK
       , Info(..)
         -- ** Queries
       , topics
       , lookup
       , channels
       , nodes
       , deleteTopic
       , tombstoneTopicProducer
       , deleteChannel
       , ping
       , info
         -- ** Reexports
       , APIError(..)
       ) where

import Control.Monad.IO.Class (MonadIO)
import Data.Aeson hiding (Result, Error)
import Data.Aeson.Casing
import Data.Text (Text)
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import Data.Word (Word16)
import GHC.Generics (Generic)
import Network.API.Builder
import Network.HTTP.Client (Manager, responseBody)
import Prelude hiding (lookup)
import Text.Read (readEither)


import Network.NSQ.Types (Channel, Topic)


-- | API monad
type NsqLookup m a = APIT () Error m a

mkBuilder :: Text -> Builder
mkBuilder url = basicBuilder "nsqlookup" url

-- | Execute 'NsqLookup' monad against nsqlookupd server 'url'
nsqlookupExec :: MonadIO m =>
                 Text -> NsqLookup m a -> m (Either (APIError Error) a)
nsqlookupExec url =
  execAPI (mkBuilder url) ()

-- | Run 'NsqLookup' monad against nsqlookupd server 'url' with 'Manager'
nsqlookupRun :: MonadIO m =>
                 Text -> Manager -> NsqLookup m a
                 -> m (Either (APIError Error) a, Builder)
nsqlookupRun url mgr q = do
  (r, b, _) <- runAPI (mkBuilder url) mgr () q
  return (r, b)

topics :: MonadIO m => NsqLookup m [Topic] 
topics =
  topicsTopicks . resultData <$> runRoute topicsRoute

lookup :: MonadIO m => Topic -> NsqLookup m Lookup
lookup topic =
  resultData <$> runRoute (lookupRoute topic)

channels :: MonadIO m => Topic -> NsqLookup m [Channel]
channels topic =
  channelsChannels . resultData <$> runRoute (channelsRoute topic)

nodes :: MonadIO m => NsqLookup m [Producer]
nodes =
  nodesProducers . resultData <$> runRoute nodesRoute

deleteTopic :: MonadIO m => Topic -> NsqLookup m OK
deleteTopic topic =
  runRoute (deleteTopicRoute topic)

tombstoneTopicProducer :: MonadIO m => Topic -> Text -> NsqLookup m OK
tombstoneTopicProducer topic node =
  runRoute (tombstoneTopicProducerRoute topic node)

deleteChannel :: MonadIO m => Topic -> Channel -> NsqLookup m OK
deleteChannel topic channel =
  runRoute (deleteChannelRoute topic channel)

ping :: MonadIO m => NsqLookup m OK
ping =
  runRoute pingRoute

info :: MonadIO m => NsqLookup m Info
info =
  resultData <$> runRoute infoRoute

topicsRoute :: Route
topicsRoute =
  Route { urlPieces = [ "topics" ]
        , urlParams = [ ]
        , httpMethod = "GET"
        }

lookupRoute :: Topic -> Route
lookupRoute topic =
  Route { urlPieces = [ "lookup" ]
        , urlParams = ["topic" =. topic]
        , httpMethod = "GET"
        }

channelsRoute :: Topic -> Route
channelsRoute topic =
  Route { urlPieces = [ "channels" ]
        , urlParams = ["topic" =. topic]
        , httpMethod = "GET"
        }

nodesRoute :: Route
nodesRoute =
  Route { urlPieces = [ "nodes" ]
        , urlParams = [ ]
        , httpMethod = "GET"
        }

deleteTopicRoute :: Topic -> Route
deleteTopicRoute topic =
  Route { urlPieces = [ "delete_topic" ]
        , urlParams = ["topic" =. topic]
        , httpMethod = "GET"
        }

tombstoneTopicProducerRoute :: Topic -> Text -> Route
tombstoneTopicProducerRoute topic node =
  Route { urlPieces = [ "tombstone_topic_producer" ]
        , urlParams = ["topic" =. topic, "node" =. node]
        , httpMethod = "GET"
        }

deleteChannelRoute :: Topic -> Channel -> Route
deleteChannelRoute topic channel =
  Route { urlPieces = [ "delete_channel" ]
        , urlParams = ["topic" =. topic, "channel" =. channel]
        , httpMethod = "GET"
        }

pingRoute :: Route
pingRoute =
  Route { urlPieces = [ "ping" ]
        , urlParams = [ ]
        , httpMethod = "GET"
        }

infoRoute :: Route
infoRoute =
  Route { urlPieces = [ "info" ]
        , urlParams = [ ]
        , httpMethod = "GET"
        }

data Result a = Result {
  resultStatusCode :: !Int
  , resultStatusTxt  :: !Text
  , resultData :: !a 
  }
            deriving (Show, Generic)

instance FromJSON a => FromJSON (Result a) where
  parseJSON = genericParseJSON $ aesonPrefix snakeCase

instance FromJSON a => Receivable (Result a) where
  receive r =
    case useFromJSON r of
      (Right Result{..}) | resultStatusCode /= 200 ->
        case receiveError r of
          Just x -> Left $ APIError x
          Nothing -> Left $ ParseError "Unknown error"
      v ->
        v

data OK = OK
            deriving (Show, Read, Generic)
instance FromJSON OK

instance Receivable OK where
  receive resp =
    case readEither . TL.unpack . TLE.decodeUtf8 $ responseBody resp of
      Left err ->
        case receiveError resp of
          Just x -> Left $ APIError x
          Nothing -> Left $ ParseError err
      Right x -> return x

data Error = Error {
  errorStatusCode :: !Int
  , errorStatusTxt  :: !Text
  }
            deriving (Show, Generic)

instance FromJSON Error where
  parseJSON = genericParseJSON $ aesonPrefix snakeCase

instance ErrorReceivable Error where
  receiveError r =
    case useErrorFromJSON r of
      v@(Just e) | errorStatusCode e /= 200 -> v
      _ -> Nothing

-- topics
newtype Topics = Topics { topicsTopicks :: [Topic] }
              deriving (Show, Generic)

instance FromJSON Topics where
  parseJSON = withObject "Topics" $ \o ->
    Topics <$> o .: "topics"

-- Lookup
data Producer = Producer {
  producerRemoteAddress :: !Text
  , producerHostname :: !Text
  , producerBroadcastAddress :: !Text
  , producerTcpPort :: !Word16
  , producerHttpPort :: !Word16
  , producerVersion :: !Text
  , producerTombstones :: !(Maybe [Bool])
  , producerTopics :: !(Maybe [Topic])
  }
                deriving (Show, Generic)

instance FromJSON Producer where
  parseJSON = genericParseJSON $ aesonPrefix snakeCase

data Lookup = Lookup {
  lookupProducers  :: ![Producer]
  , lookupChannels :: ![Channel]
  }
              deriving (Show, Generic)

instance FromJSON Lookup where
  parseJSON = genericParseJSON $ aesonPrefix snakeCase

-- channels
newtype Channels = Channels { channelsChannels :: [Channel] }
              deriving (Show, Generic)

instance FromJSON Channels where
  parseJSON = withObject "Channels" $ \o ->
    Channels <$> o .: "channels"

-- nodes
newtype Nodes = Nodes { nodesProducers :: [Producer] }
              deriving (Show, Generic)

instance FromJSON Nodes where
  parseJSON = withObject "Nodes" $ \o ->
    Nodes <$> o .: "producers"

-- info
data Info = Info {
  infoVersion :: !Text
  }
          deriving (Show, Generic)

instance FromJSON Info where
  parseJSON = genericParseJSON $ aesonPrefix snakeCase
