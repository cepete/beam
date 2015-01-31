{-# LANGUAGE GADTs #-}
module Database.Beam.Backend where

import Database.Beam.Types
import Database.Beam.Schema
import Database.Beam.SQL.Types
import Database.Beam.SQL

import Control.Arrow
import Control.Applicative
import Control.Monad.Trans
import Control.Monad.Writer
import Control.Monad

import Data.String
import Data.Maybe
import Data.List
import Data.Proxy
import qualified Data.Set as S

import Database.HDBC

instance Monoid DBSchemaComparison where
    mappend (Migration a) (Migration b) = Migration (a <> b)
    mappend _ Unknown = Unknown
    mappend Unknown _ = Unknown

    mempty = Migration []

defaultBeamCompareSchemas :: ReifiedDatabaseSchema -> Database -> DBSchemaComparison
defaultBeamCompareSchemas actual db = execWriter compare
    where expected = reifyDBSchema db

          actualTableSet = S.fromList (map fst actual)
          expTableSet = S.fromList (map fst expected)
          expGenTables = tables db

          tablesToBeMade = expTableSet S.\\ actualTableSet

          genTablesToBeMade = mapMaybe lookupGenTable (S.toList tablesToBeMade)
              where lookupGenTable tblName = find (\(GenTable t) -> dbTableName t == tblName) expGenTables

          compare = tell (Migration (map (\(GenTable t) -> MACreateTable t) genTablesToBeMade))

hdbcSchema :: (IConnection conn, MonadIO m) => conn -> m ReifiedDatabaseSchema
hdbcSchema conn =
    liftIO $
    do tables <- getTables conn
       forM tables $ \tbl ->
           do descs <- describeTable conn tbl
              return (fromString tbl, map (fromString *** noConstraints) descs)

createStmtFor :: (Table t) => Beam m -> Proxy t -> SQLCreateTable
createStmtFor beam table =
    let tblSchema = reifyTableSchema table
        tblSchemaInDb' = map (second (adjustColDescForBackend beam)) (reifyTableSchema table)
    in SQLCreateTable (dbTableName table) (tblSchemaInDb')

migrateDB :: MonadIO m => db -> Beam m -> [MigrationAction] -> m ()
migrateDB db beam actions =
  forM_ actions $ \action ->
      do liftIO (putStrLn (concat ["Performing ", show action]))

         case action of
           MACreateTable t -> do let stmt = createStmtFor beam t
                                     (sql, vals) = ppSQL (CreateTable stmt)
                                 liftIO (putStrLn (concat ["Will run SQL:\n", sql]))
                                 withHDBCConnection beam (\conn -> liftIO $ do runRaw conn sql
                                                                               commit conn)
                                 liftIO (putStrLn "Done...")

autoMigrateDB db beam =
    do actDBSchema <- withHDBCConnection beam hdbcSchema
       let comparison = compareSchemas beam actDBSchema db

       case comparison of
         Migration actions -> do liftIO $ putStrLn (concat ["Comparison result: ", show actions])
                                 migrateDB db beam actions
         Unknown -> liftIO $ putStrLn "Unknown comparison"

openDatabase :: (BeamBackend dbSettings, MonadIO m) => Database -> dbSettings -> m (Beam m)
openDatabase db dbSettings =
  do beam <- openBeam dbSettings
     autoMigrateDB db beam

     return beam