module Database.Beam.Backend.PostgreSQL where

import Database.Beam.Internal
import Database.Beam.Query.Internal
import Database.Beam.Backend
import Database.Beam.SQL.Types

import Control.Monad.Trans

import Database.HDBC.PostgreSQL
import Database.HDBC

import Data.Text (Text, unpack)

-- * PostgreSQL support

data PostgreSQLSettings = PostgreSQLSettings FilePath
                       deriving Show

instance BeamBackend PostgreSQLSettings where
    openBeam dbSettings (PostgreSQLSettings fp) =
        do conn <- liftIO (connectPostgreSQL fp)

           return Beam { beamDbSettings = dbSettings
                       , beamDebug = False
                       , closeBeam = liftIO (disconnect conn)
                       , compareSchemas = defaultBeamCompareSchemas
                       , adjustColDescForBackend =
                           \cs -> cs { csConstraints = filter (/=SQLAutoIncrement) (csConstraints cs) }
                       , getLastInsertedRow = getLastInsertedRow' conn
                       , withHDBCConnection = \f -> f conn }

getLastInsertedRow' :: MonadIO m => Connection -> Text -> m [SqlValue]
getLastInsertedRow' conn tblName = do
  [res] <- liftIO (quickQuery conn (concat ["SELECT * FROM ", unpack tblName, " WHERE ROWID=(SELECT last_insert_rowid()) limit 1"]) [])
  return res


(++.) :: QExpr s Text -> QExpr s Text -> QExpr s Text
QExpr a ++. QExpr b = QExpr (SQLBinOpE "||" a b)
