{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE StrictData #-}

module Main
  (main)
where

import qualified BlueRipple.Configuration as BR
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Model.Demographic.DataPrep as DDP
import qualified BlueRipple.Model.Demographic.EnrichData as DED
import qualified BlueRipple.Model.Demographic.TableProducts as DTP
import qualified BlueRipple.Model.Demographic.TPModel3 as DTM3
import qualified BlueRipple.Model.Demographic.EnrichCensus as DMC
import qualified BlueRipple.Model.Demographic.MarginalStructure as DMS
import qualified BlueRipple.Model.Demographic.NullSpaceBasis as DNS
import qualified BlueRipple.Data.Keyed as Keyed

import qualified BlueRipple.Data.ACS_PUMS as ACS
import qualified BlueRipple.Data.ACS_Tables as ACS
import qualified BlueRipple.Data.ACS_Tables_Loaders as ACS
import qualified BlueRipple.Data.Types.Demographic as DT
import qualified BlueRipple.Data.Types.Geographic as GT
import qualified BlueRipple.Data.Small.DataFrames as BRDF
import qualified BlueRipple.Data.Small.Loaders as BRDF
import qualified BlueRipple.Data.CachingCore as BRCC
import qualified BlueRipple.Data.LoadersCore as BRLC
import qualified BlueRipple.Data.Keyed as BRK

import qualified Knit.Report as K
import qualified Knit.Utilities.Streamly as KS
import qualified Knit.Effect.AtomicCache as KC
import qualified Polysemy.RandomFu as PR
import qualified Data.Random as R
import qualified System.Random as SR
import qualified Text.Pandoc.Error as Pandoc
import qualified System.Console.CmdArgs as CmdArgs
import qualified Colonnade as C

import GHC.TypeLits (Symbol)

import Control.Lens (Lens', view)

import qualified Flat

import qualified Data.List as List
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Strict as M
import qualified Data.Map.Merge.Strict as MM
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as VS
import qualified Data.Vinyl as V
import qualified Data.Vinyl.Functor as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Numeric
import qualified Numeric.LinearAlgebra as LA
import qualified Control.Foldl as FL
import qualified Control.Foldl.Statistics as FLS
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Frames.Transform as FT
import qualified Frames.Streamly.InCore as FSI
import qualified Frames.Streamly.CSV as FCSV
import qualified Frames.Streamly.Streaming.Streamly as FSS
import Frames.Streamly.Streaming.Streamly (StreamlyStream, Stream)
import Frames.Streamly.Streaming.Class (StreamFunctions(..), StreamFunctionsIO(..))
import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Prelude as StreamlyP
import qualified Streamly.Data.Stream.Prelude as Streamly
import qualified Control.MapReduce as MR
import qualified Frames.MapReduce as FMR

import Control.Lens (view, (^.), _2)

import Path (Dir, Rel)
import qualified Path

import qualified Text.Printf as PF
import qualified Graphics.Vega.VegaLite as GV
import qualified Graphics.Vega.VegaLite.Compat as FV
import qualified Graphics.Vega.VegaLite.Configuration as FV
import qualified Graphics.Vega.VegaLite.JSON as VJ

import qualified System.Environment as Env

import qualified Numeric.NNLS.LH as LH

import GHC.Debug.Stub (withGhcDebug)

templateVars ∷ M.Map String String
templateVars =
  M.fromList
    [ ("lang", "English")
    , ("site-title", "Blue Ripple Politics")
    , ("home-url", "https://www.blueripplepolitics.org")
    --  , ("author"   , T.unpack yamlAuthor)
    ]

pandocTemplate ∷ K.TemplatePath
pandocTemplate = K.FullySpecifiedTemplatePath "../../research/pandoc-templates/blueripple_basic.html"

data CountWithDensity = CountWithDensity { cwdN :: Int, cwdD ::  Double} deriving stock (Show, Eq)

instance Semigroup CountWithDensity where
  (CountWithDensity n1 d1) <> (CountWithDensity n2 d2) = CountWithDensity sumN avgDens
    where
      sumN = n1 + n2
      avgDens = (realToFrac n1 * d1 + realToFrac n2 * d2) / realToFrac sumN

instance Monoid CountWithDensity where
  mempty = CountWithDensity 0 0

logTime :: K.KnitEffects r => Text -> K.Sem r a -> K.Sem r a
logTime = K.logTiming (K.logLE K.Info)

{-
recToCWD :: F.Record [DT.PopCount, DT.PWPopPerSqMile] -> CountWithDensity
recToCWD r = CountWithDensity (r ^. DT.popCount) (r ^. DT.pWPopPerSqMile)

cwdToRec :: CountWithDensity -> F.Record [DT.PopCount, DT.PWPopPerSqMile]
cwdToRec (CountWithDensity n d) = n F.&: d F.&: V.RNil

updateCWDCount :: Int -> CountWithDensity -> CountWithDensity
updateCWDCount n (CountWithDensity _ d) = CountWithDensity n d
-}

--type InputFrame ks = F.FrameRec (ks V.++ '[DT.PopCount, DT.PWPopPerSqMile])
type ResultFrame ks = F.FrameRec (ks V.++ '[DT.PopCount])

data MethodResult ks = MethodResult { mrResult :: ResultFrame ks, _mrTableTitle :: Maybe Text, mrChartTitle :: Maybe Text, mrErrHeader :: Maybe Text}

data ErrorFunction a = ErrorFunction { errLabel :: Text, errF :: a -> a -> Double, errTableFmt :: Double -> Text, errChartScale :: Double -> Double }

data ErrorResults rk a = ErrorResults { errSourceLabel :: Text, errByRegionMap :: Map rk a, errByCell :: [a] }

sortByIndexList :: [Int] -> [a] -> [a]
sortByIndexList indexOrder'  = fmap snd . sortOn fst . zip indexOrder'

indexOrder :: (a -> a -> Ordering) -> [a] -> [Int]
indexOrder f = fmap fst . sortBy (\(_, a1) (_, a2) -> f a1 a2) . zip [0..]

compareResults :: forall ks key colKey rowKey a r f .
                  (K.KnitOne r
                  , Traversable f
                  , Ord key
                  , Keyed.FiniteSet colKey
                  , Show colKey
                  , Ord colKey
                  , Keyed.FiniteSet rowKey
                  , Show rowKey
                  , Ord rowKey
                  , Ord (F.Record ks)
                  , V.RecordToList ks
                  , ks F.⊆ (ks V.++ '[DT.PopCount])
                  , V.ReifyConstraint Show F.ElField ks
                  , V.RMap ks
                  , F.ElemOf (ks V.++ '[DT.PopCount]) DT.PopCount
                  , FSI.RecVec (ks V.++ '[DT.PopCount])
                  , F.ElemOf (ks V.++ '[DT.PopCount]) GT.StateAbbreviation
                  , Ord a
                  , Show a
                  )
               => BR.PostPaths Path.Abs
               -> BR.PostInfo
               -> Bool
               -> Bool
               -> Text
               -> (Text, Map a Int, F.Record ks -> a)
               -> Maybe Text
               -> (F.Record (ks V.++ '[DT.PopCount]) -> key)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> rowKey)
               -> (F.Record (ks V.++ '[DT.PopCount]) -> colKey)
               -> (F.Record ks -> Text)
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> Maybe (Text, F.Record ks -> Text, Maybe [Text])
               -> ErrorFunction (VS.Vector Double)
               -> ErrorFunction (VS.Vector Double)
               -> ResultFrame ks
               -> f (MethodResult ks)
               -> K.Sem r ()
compareResults pp pi' showTables showScatters chartDataPrefix (regionType, regionPopMap, regionKey) exampleStateM
  catKey rowKey colKey showCellKey colorM shapeM byRegionErrorF byCellErrorF actual results = do
  let tableText d = toText (C.ascii (fmap toString $ mapColonnade)
                             $ FL.fold (fmap DED.totaledTable
                                         $ DED.rowMajorMapFldInt
                                         (view DT.popCount)
                                         rowKey
                                         colKey
                                       )
                             d
                           )
      allRegions = M.keys regionPopMap
      facetB = True
      asDiffB = True
      aspect = if asDiffB then 1.33 else 1
      hvHeight = if facetB then 100 else 500
      hvWidth = aspect * hvHeight
      hvPad = if facetB then 1 else 5
  let compChartM ff pctPop ls t l fM = do
        let logF = if pctPop then Numeric.log else Numeric.logBase 10
            title = case (ls, pctPop) of
              (False, False) -> t
              (True, False) -> t <> " (Log scale)"
              (False, True) -> t <> " (% Pop)"
              (True, True) -> t <> " (% Pop, Log scale)"
        case fM of
          Just f -> do
            vl <- distCompareChart pp pi' chartDataPrefix
                  (FV.fixedSizeVC hvWidth hvHeight hvPad)
                  title
                  (F.rcast @ks)
                  showCellKey
                  facetB
                  colorM
                  shapeM
                  asDiffB
                  ((if ls then logF else id) . realToFrac . view DT.popCount)
                  (if pctPop then (Just (regionKey, regionPopMap)) else Nothing)
                  ("Actual ACS", ff actual)
                  (l, ff f)
            _ <- K.addHvega Nothing Nothing vl
            pure ()
          Nothing -> pure ()


--      compChartMF f pp ls
  let title x = if asDiffB then (x <> " - Actual vs " <> x) else (x <> " vs Actual" )
  case exampleStateM of
    Nothing -> pure ()
    Just sa -> do
      let ff = F.filterFrame (\r -> r ^. GT.stateAbbreviation == sa)
          tableTextMF t rfM = whenJust rfM $ \rf -> K.logLE K.Info $ t <> "\n" <> tableText (ff rf)
          stTitle x = title x <> ": " <> sa
      K.logLE K.Info $ "Example State=" <> sa
      when showTables $ do
        tableTextMF "Actual Joint" (Just actual)
        traverse_ (\mr -> maybe (pure ()) (\t -> tableTextMF t (Just $ mr.mrResult)) $ mr.mrChartTitle) results
      when showScatters $ do
        K.logLE K.Info "Building example state charts."
        traverse_ (\mr -> maybe (pure ()) (\t -> compChartM ff False False (stTitle t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
        traverse_ (\mr -> maybe (pure ()) (\t -> compChartM ff False True (stTitle t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
      pure ()

  -- compute errors

  let toVecFld :: FL.Fold (F.Record (ks V.++ '[DT.PopCount])) (VS.Vector Double)
      toVecFld = VS.fromList . fmap snd . M.toList <$> FL.premap (\r -> (catKey r, realToFrac (r ^. DT.popCount))) FL.map
--      toVecF = VS.fromList . fmap snd . M.toList . FL.fold (FL.premap (\r -> (catKey r, realToFrac (r ^. DT.popCount))) FL.map)
--      toVecF = FL.fold toVecFld
  let vecMapFld :: FL.Fold (F.Record (ks V.++ '[DT.PopCount])) (Map a (VS.Vector Double))
      vecMapFld = M.fromList
                  <$> MR.mapReduceFold
                  MR.noUnpack
                  (MR.assign (regionKey . F.rcast) id)
                  (MR.foldAndLabel toVecFld (,))
      actualVecMap = FL.fold vecMapFld actual
      rowsToCols = LA.toColumns . LA.fromRows
      compMapToCellLists :: Map a (VS.Vector Double, VS.Vector Double) -> [(VS.Vector Double, VS.Vector Double)]
      compMapToCellLists m = uncurry zip $ bimap rowsToCols rowsToCols $ unzip $ M.elems m
      compMap dName dVecMap = do
        let whenMatched _ c1 c2 = Right (c1, c2)
            whenMissing t1 t2 k _ = Left $ show k <> " in " <> t1 <> " is missing from " <> t2
        K.knitEither $ MM.mergeA
          (MM.traverseMissing $ whenMissing "Actual" dName)
          (MM.traverseMissing $ whenMissing dName "Actual")
          (MM.zipWithAMatched $ whenMatched)
          actualVecMap
          dVecMap
      resultToErrsM (MethodResult rf _ _ ehM) =
        case ehM of
          Nothing -> pure Nothing
          Just h -> do
            byRegionErrMap <- fmap (uncurry byRegionErrorF.errF) <$> compMap h (FL.fold vecMapFld rf)
            byCellErrList <- fmap (uncurry byCellErrorF.errF) . compMapToCellLists <$> compMap h (FL.fold vecMapFld rf)
            pure $ Just $ ErrorResults h byRegionErrMap byCellErrList
  K.logLE K.Info "Computing errors by region and by cell..."
  resultErrs <- catMaybes . FL.fold FL.list <$> traverse resultToErrsM results
  for_ resultErrs $ \(ErrorResults s brm _) -> do
    let avgError = FL.fold FLS.mean (M.elems brm)
    K.logLE K.Info $ "<RegionError(" <> s <> ")>=" <> show avgError
  let errCols = fmap (M.elems . errByRegionMap) resultErrs
      errColHeaders = errSourceLabel <$> resultErrs
      byRegionErrTableRows = zipWith (\region errs -> ErrTableRow region errs) (fmap show allRegions) (transp errCols)
      byCellErrTableRows = zipWith (\cell errs -> ErrTableRow cell errs) [1..] (transp $ fmap errByCell resultErrs)
--      cellSizeIndexOrder = indexOrder compare $ FL.fold (FL.premap (view DT.popCount) FL.list) actual
--      byCellSizeErrTableRows = zipWith (\cell errs -> ErrTableRow cell errs) [1..] (transp $ sortByIndexList cellSizeIndexOrder $ fmap errByCell resultErrs)
--  K.logLE K.Info $ "IndexOrder = " <> show cellSizeIndexOrder
  K.logLE K.Info "Building error-comparison histogram..."
  byRegionErrCompChartVL <- errorCompareHistogram pp pi' (byRegionErrorF.errLabel <> " Histogram")
                            chartDataPrefix (FV.fixedSizeVC 400 400 5) regionType errColHeaders
                            (byRegionErrorF.errLabel, byRegionErrorF.errChartScale) byRegionErrTableRows
  _ <- K.addHvega Nothing Nothing byRegionErrCompChartVL
  K.logLE K.Info "Building error comparison XY scatter..."
  byRegionXYChart <- errorCompareXYScatter pp pi' (byRegionErrorF.errLabel <> " XY") chartDataPrefix (FV.fixedSizeVC 600 600 5)
                     regionType errColHeaders (byRegionErrorF.errLabel, byRegionErrorF.errChartScale) byRegionErrTableRows
  _ <- K.addHvega Nothing Nothing byRegionXYChart

{-      allPairs :: [q] -> [(q, q)]
      allPairs qs = go qs [] where
        go [] ps = ps
        go (q:[]) ps = ps
        go (q:qs) ps = go qs (ps ++ fmap (q,) qs)
-}

--  _ <- traverse (uncurry byRegionXY) $ allPairs $ zip errColHeaders byRegionErrTableRows
  K.logLE K.Info "Building error comparison by cell..."
  byCellErrCompChartVL <- errorCompareScatter pp pi' (byCellErrorF.errLabel <> " by Cell")
                          chartDataPrefix (FV.fixedSizeVC 400 400 5) "Cell" errColHeaders
                          (byCellErrorF.errLabel, byCellErrorF.errChartScale) byCellErrTableRows
  _ <- K.addHvega Nothing Nothing byCellErrCompChartVL
{-
  byCellSizeErrCompChartVL <- errorCompareScatter pp pi' (byCellErrorF.errLabel <> " by CellSize")
                              (chartDataPrefix <> "bySize") (FV.fixedSizeVC 400 400 5) "Cell Size in Actual" errColHeaders
                              (byCellErrorF.errLabel, byCellErrorF.errChartScale) byCellSizeErrTableRows
  _ <- K.addHvega Nothing Nothing byCellSizeErrCompChartVL
-}

  K.logLE K.Info "Computing per position Errors..."
{-
       computeErr d rk = errF acsV dV where
        acsV = toVecF $ F.filterFrame ((== rk) . regionKey . F.rcast) actual
        dV = toVecF $ F.filterFrame ((== rk) . regionKey . F.rcast) d

      errCol mr = case mr.mrErrHeader of
        Just h -> Just $ (h, computeErr mr.mrResult <$> allRegions)
        Nothing -> Nothing
      errCols = catMaybes $ FL.fold FL.list $ fmap errCol results
      errColHeaders = fst <$> errCols
      errTableRows = zipWith (\region kls -> ErrTableRow region kls) (fmap show allRegions) (transp $ fmap snd errCols)
  errCompChartVL <- errorCompareChart pp pi' errLabel chartDataPrefix (FV.ViewConfig 400 400 5) regionType errColHeaders (errLabel, errScale) errTableRows
  _ <- K.addHvega Nothing Nothing errCompChartVL
-}

  when showTables $ do
    K.logLE K.Info "Error Table:"
    K.logLE K.Info $ "\n" <> toText (C.ascii (fmap toString $ errColonnadeFlex byRegionErrorF.errTableFmt errColHeaders "Region") byRegionErrTableRows)
--  _ <- compChart True "Actual" $ mrActual results
  when showScatters $ do
    K.logLE K.Info "Building national charts."
    traverse_ (\mr -> maybe (pure ()) (\t -> compChartM id False False (title t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
    traverse_ (\mr -> maybe (pure ()) (\t -> compChartM id False True (title t) t $ Just mr.mrResult) $ mr.mrChartTitle) results
  pure ()

pctFmt :: Double -> Text
pctFmt = toText @String . PF.printf "%2.1g" . (100 *)

stdMeanErr :: LA.Vector Double -> LA.Vector Double -> Double
stdMeanErr acsV dV =
  let x = lmvsk acsV dV
      m = FLS.lmvskMean x
      v = FLS.lmvskVariance x
  in m / sqrt v

l1Err :: LA.Vector Double -> LA.Vector Double -> Double
l1Err acsV dV = FL.fold
                ((/)
                  <$> FL.premap fst FL.sum
                  <*> FL.premap snd FL.sum)
                $ zipWith (\acs d' -> (abs (acs - d') / 2, acs)) (VS.toList acsV) (VS.toList dV)

l2Err :: LA.Vector Double -> LA.Vector Double -> Double
l2Err acsV dV = FL.fold
                ((\sumSqDiff s -> sqrt sumSqDiff / s)
                  <$> FL.premap fst FL.sum
                  <*> FL.premap snd FL.sum)
                $ zipWith (\acs d -> ((acs - d) * (acs - d), acs)) (VS.toList acsV) (VS.toList dV)

lmvsk :: LA.Vector Double -> LA.Vector Double -> FLS.LMVSK
lmvsk acsV dV = FL.fold FLS.fastLMVSK $ VS.toList (acsV - dV)

transp :: [[a]] -> [[a]]
transp = go [] where
  go x [] = fmap reverse x
  go [] (r : rs) = go (fmap (: []) r) rs
  go x (r :rs) = go (List.zipWith (:) r x) rs


--type SER = [DT.SexC, DT.Education4C, DT.Race5C]
--type ASR = [DT.Age4C, DT.SexC, DT.Race5C]
--type ASER = [DT.Age4C, DT.SexC, DT.Education4C, DT.Race5C]

--type A5SR = [DT.Age5FC, DT.SexC, DT.Race5C]
--type A5SER = [DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]

type CDLoc = [BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict]
type TestRow ok ks =  ok V.++ DMC.KeysWD ks


-- NB: as ks - first component and bs is ks - second
-- E.g. if you are combining ASR and SER to make ASER ks=ASER, as=E, bs=A
testNS :: forall  outerK ks (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) qs r .
           (K.KnitEffects r, BRCC.CacheEffects r
           , DMC.PredictorModelC as bs (qs V.++ as V.++ bs) qs
           , DMC.PredictedTablesC outerK qs as bs
           , qs ~ F.RDeleteAll (as V.++ bs) ks
           , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
           , qs V.++ as F.⊆ DMC.PUMARowR ((qs V.++ as) V.++ bs)
           , qs V.++ bs F.⊆ DMC.PUMARowR ((qs V.++ as) V.++ bs)
           , ((qs V.++ as) V.++ bs) F.⊆ DMC.PUMARowR ((qs V.++ as) V.++ bs)
           , DMC.KeysWD (qs V.++ as V.++ bs) F.⊆ DMC.PUMARowR ks
           , AggregateAndZeroFillC outerK (F.RDeleteAll as ks)
           , AggregateAndZeroFillC outerK (F.RDeleteAll bs ks)
           , DMC.PredictedTablesC outerK qs as bs
           , (qs V.++ as V.++ bs) F.⊆ TestRow outerK ks
           , (qs V.++ bs) F.⊆ TestRow outerK ks
           , (qs V.++ as) F.⊆ TestRow outerK ks
           , TestRow outerK (F.RDeleteAll bs ks) F.⊆ TestRow outerK ks
           , TestRow outerK (F.RDeleteAll as ks) F.⊆ TestRow outerK ks
           , TestRow outerK (qs V.++ as) F.⊆ TestRow outerK (F.RDeleteAll bs ks)
           , TestRow outerK (qs V.++ bs) F.⊆ TestRow outerK (F.RDeleteAll as ks)
           , outerK F.⊆ TestRow outerK ks
           , F.ElemOf outerK GT.StateAbbreviation
           , F.ElemOf (TestRow outerK ks) DT.PopCount
           , F.ElemOf (TestRow outerK ks) DT.PWPopPerSqMile
           , TestRow outerK ks F.⊆ TestRow outerK (qs V.++ as V.++ bs)
           , (qs V.++ as) V.++ bs F.⊆ ((qs V.++ as) V.++ bs)
           , ((qs V.++ as) V.++ bs) F.⊆ ks
           )
       => DTP.OptimalOnSimplexF r --(forall k . DTP.NullVectorProjections k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double))
       -> Either Text Text
       -> Either Text Text
       -> (Int -> DTM3.MeanOrModel)
       -> (F.Record outerK -> Text)
       -> BR.CommandLine
       -> Maybe (LA.Matrix Double)
       -> Maybe (Text, DNS.CatsAndKnowns (F.Record ks))
       -> Maybe Double
       -> K.ActionWithCacheTime r (F.FrameRec (DMC.PUMARowR ks))
       -> K.ActionWithCacheTime r (F.FrameRec (TestRow outerK ks))
       -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (TestRow outerK ks), F.FrameRec (TestRow outerK ks)))
testNS onSimplexM modelIdE predictionCacheDirE meanOrModelF testRowKeyText cmdLine amM seM fracVarM byPUMA_C test_C = do
--  productFrameCacheKey <- BRCC.cacheFromDirE predictionCacheDirE  "productFrame.bin"
  let   seM' :: Maybe (Text, DNS.CatsAndKnowns (F.Record (qs V.++ as V.++ bs)))
        seM' = fmap (second $ DNS.reTypeCatsAndKnowns) seM
        numCovariates = S.size (Keyed.elements @(F.Record (qs V.++ as))) + S.size (Keyed.elements @(F.Record (qs V.++ bs)))
  (predictor_C, ms) <- DMC.predictorModel3 @as @bs @(qs V.++ as V.++ bs) @qs
    modelIdE predictionCacheDirE (meanOrModelF $ numCovariates + 1) False amM seM' fracVarM $ fmap (fmap F.rcast) byPUMA_C -- +1 for pop density
  let testAs_C = (aggregateAndZeroFillTables @outerK @(F.RDeleteAll bs ks) . fmap F.rcast) <$> test_C
      testBs_C = (aggregateAndZeroFillTables @outerK @(F.RDeleteAll as ks) . fmap F.rcast) <$> test_C
--      iso :: DMS.IsomorphicKeys (F.Record ks) (F.Record (qs V.++ as V.++ bs)) = DMS.IsomorphicKeys F.rcast F.rcast
--  K.logLE K.Diagnostic $ "testNS: list permutation of [1,2,3,..] is " <> (show $ DTP.permuteList iso [1..])
  (predictions_C, products_C) <- DMC.predictedTables @outerK @qs @as @bs
                                 DMS.PosConstrainedDensity
                                 onSimplexM
                                 predictionCacheDirE
                                 (pure . view GT.stateAbbreviation)
                                 predictor_C
                                 (fmap F.rcast <$> testAs_C)
                                 (fmap F.rcast <$> testBs_C)

  when (BR.logLevel cmdLine > BR.LogDebugMinimal) $ do
    predictor <- K.ignoreCacheTime predictor_C
    test <- K.ignoreCacheTime test_C
    let ptd = DTM3.predPTD predictor
        nvps = DTP.nullVectorProjections ptd
        testModelData = FL.fold
          (DTM3.nullVecProjectionsModelDataFldCheck
           DMS.cwdWgtLens
           ms
           nvps
           (F.rcast @outerK)
           (F.rcast @(qs V.++ as V.++ bs))
           DTM3.cwdF
           (DMC.innerFoldWD @(qs V.++ as) @(qs V.++ bs) @(TestRow outerK ks) (F.rcast @(qs V.++ as)) (F.rcast @(qs V.++ bs)))
          )
          test
        cMatrix =  DTP.nvpConstraints (DTP.nullVectorProjections $ DTM3.predPTD predictor) --DED.mMatrix (DMS.msNumCategories ms) (DMS.msStencils ms)
    forM_ testModelData $ \(sar, md, nVpsActual, pKWs, oKWs) -> do
      let keyT = testRowKeyText sar
          pV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) pKWs
          nV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) oKWs
          n = VS.sum pV
          showSum v = " (" <> show (VS.sum v) <> ")"
      K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> showSum nV
      K.logLE K.Info $ keyT <> " prod counts=" <> DED.prettyVector pV <> showSum pV
      let nvpsV = DTP.applyNSPWeights (DTM3.predPTD predictor) (VS.map (* n) nVpsActual) pV
      let cCheckV = cMatrix LA.#> (nV - pV)
      K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector cCheckV <> showSum cCheckV
      K.logLE K.Info $ keyT <> " predictors: " <> DED.prettyVector md
      nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM3.modelResultNVPs predictor (view GT.stateAbbreviation sar) md)
      K.logLE K.Info $ keyT <> " projections=" <> DED.prettyVector nVpsActual
      K.logLE K.Info $ "sumSq(projections)=" <> show (VS.sum $ VS.zipWith (*) nVpsActual nVpsActual)
      K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled <> showSum nvpsModeled
      K.logLE K.Info $ "sumSq(modeled)=" <> show (VS.sum $ VS.zipWith (*) nvpsModeled nvpsModeled)
      let dNVPs = VS.zipWith (-) nVpsActual nvpsModeled
      K.logLE K.Info $ "dNVPS=" <> DED.prettyVector dNVPs
      K.logLE K.Info $ "sumSq(dNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dNVPs dNVPs)
      simplexFull <- onSimplexM ptd nvpsModeled $ VS.map (/ n) pV
      let simplexNVPs = DTP.fullToProj nvps simplexFull
          dSimplexNVPs = VS.zipWith (-) simplexNVPs nVpsActual
      K.logLE K.Info $ "dSimplexNVPS=" <> DED.prettyVector dSimplexNVPs
      K.logLE K.Info $ "sumSq(dSimplexNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dSimplexNVPs dSimplexNVPs)
      K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs <> showSum simplexNVPs
      let modeledCountsV = VS.map (* n) simplexFull
      K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector modeledCountsV <> showSum modeledCountsV
      let maCheckV = cMatrix LA.#> (nV - modeledCountsV)
      K.logLE K.Info $ keyT <> " C * (modeled - actual)=" <> DED.prettyVector maCheckV
      let mpCheckV = cMatrix LA.#> (modeledCountsV - pV)
      K.logLE K.Info $ keyT <> " C * (modeled - product)=" <> DED.prettyVector mpCheckV

  pure $ (,) <$> (fmap (fmap F.rcast) products_C) <*> (fmap (fmap F.rcast) predictions_C)

testRowsWithZeros :: forall outerK ks .
                     (
                       (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) F.⊆ TestRow outerK ks
                     , ks F.⊆ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                     , FSI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                     , Keyed.FiniteSet (F.Record ks)
                     , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PWPopPerSqMile
                     , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PopCount
                     , Ord (F.Record outerK)
                     , outerK F.⊆ TestRow outerK ks
                     , FSI.RecVec (outerK V.++ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                     )
                  => F.FrameRec (TestRow outerK ks)
                  -> F.FrameRec (TestRow outerK ks)
testRowsWithZeros frame = FL.fold (FMR.concatFold
                           $ FMR.mapReduceFold
                           FMR.noUnpack
                           (FMR.assignKeysAndData @outerK @(ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                           (FMR.foldAndLabel
                            (fmap F.toFrame $ Keyed.addDefaultRec @ks zc)
                            (\k r -> fmap (k F.<+>) r)
                           )
                          )
                  frame
  where
       zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil


type AggregateAndZeroFillC ls ks =
  (
  (ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile] ~ ls V.++ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
  , Ord (F.Record ls)
  , ls F.⊆ ((ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
  , ks F.⊆ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
  , ks V.++ [DT.PopCount, DT.PWPopPerSqMile] F.⊆ ((ls V.++ ks) V.++ [DT.PopCount, DT.PWPopPerSqMile])
  , FSI.RecVec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
  , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PopCount
  , F.ElemOf (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]) DT.PWPopPerSqMile
  , Keyed.FiniteSet (F.Record ks)
  , Ord (F.Record ks)
  , FSI.RecVec (ls V.++ (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
  )
aggregateAndZeroFillTables :: forall ls ks . AggregateAndZeroFillC ls ks
                           => F.FrameRec (ls V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
                           -> F.FrameRec (ls V.++ ks V.++ [DT.PopCount, DT.PWPopPerSqMile])
aggregateAndZeroFillTables frame =FL.fold
                                  (FMR.concatFold
                                   $ FMR.mapReduceFold
                                   FMR.noUnpack
                                   (FMR.assignKeysAndData @ls @(ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
                                   (FMR.foldAndLabel innerFld (\k r -> fmap (k F.<+>) r))
                                  )
                                  frame
  where
    toKW r = (F.rcast @ks r, DTM3.cwdF r)
    toRec (k, w) = k F.<+> DMS.cwdToRec w
    innerFld :: FL.Fold (F.Record (ks V.++ [DT.PopCount, DT.PWPopPerSqMile])) (F.FrameRec (ks V.++ [DT.PopCount, DT.PWPopPerSqMile]))
    innerFld = fmap (F.toFrame . fmap toRec . M.toList) $ FL.premap toKW DMS.zeroFillSummedMapFld


tsModelConfig modelId n =  DTM3.ModelConfig True (DTM3.dmr modelId n)
                           DTM3.AlphaHierNonCentered DTM3.ThetaSimple DTM3.NormalDist

thModelConfig modelId n =  DTM3.ModelConfig True (DTM3.dmr modelId n)
                           DTM3.AlphaHierNonCentered DTM3.ThetaHierarchical DTM3.NormalDist

{-
compareCSR_ASR_ASE :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareCSR_ASR_ASE cmdLine postInfo = do
    K.logLE K.Info "Building test PUMA-level products for CSR x ASR -> CASR x ASE -> CASER -> ASER"
    let --filterToState sa r = r ^. GT.stateAbbreviation == sa
        pumaKey r = (r ^. GT.stateAbbreviation, r ^. GT.pUMA)
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA ACS.acs1Yr2010_20 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C
{-    testCDs_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.CASR . fmap F.rcast)
                 <$> DDP.cachedACSa5ByCD
    testCDs <- K.ignoreCacheTime testCDs_C
-}
    pumaTest_CASR_C <- testNS @DMC.PUMAOuterKeyR @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                       (DTP.viaOptimalWeights DTP.euclideanFull)
                       (Right "CSR_ASR_ByPUMA")
                       (Right "model/demographic/csr_asr_PUMA")
                       (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                       (view GT.stateAbbreviation) cmdLine Nothing Nothing Nothing (fmap (fmap F.rcast) byPUMA_C) (fmap (fmap F.rcast) testPUMAs_C)
--    (pumaProduct_CASR, pumaModeled_CASR) <- K.ignoreCacheTime pumaTest_CASR_C

    (ascrePredictor_C, ascre_ms) <- DMC.predictorModel3 @[DT.CitizenC, DT.Race5C] @'[DT.Education4C] @DMC.ASCRE @DMC.AS
                                    (Right "CASR_ASE_ByPUMA") (Right "model/demographic/casr_ase") (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                                    Nothing Nothing Nothing $ fmap (fmap F.rcast) byPUMA_C
    let casr_C =  {- (aggregateAndZeroFillTables @DMC.PUMAOuterKeyR @DMC.ASCR . fmap F.rcast) <$> -} fmap snd pumaTest_CASR_C
        ase_C = (aggregateAndZeroFillTables @DMC.PUMAOuterKeyR @DMC.ASE . fmap F.rcast) <$> byPUMA_C

    (pumaModeled_ASCRE_C, pumaProduct_ASCRE_C) <- DMC.predictedTables @DMC.PUMAOuterKeyR @DMC.AS @[DT.CitizenC, DT.Race5C] @'[DT.Education4C]
                                                 (DTP.viaOptimalWeights DTP.euclideanFull)
                                                 (Right "model/demographic/csr_asr_ase_PUMA")
                                                 (pure  . view GT.stateAbbreviation)
                                                 ascrePredictor_C
                                                 (fmap (fmap F.rcast) casr_C) -- make it ASCR
                                                 ase_C

    let isCitizen :: F.ElemOf rs DT.CitizenC => F.Record rs -> Bool
        isCitizen r = r ^. DT.citizenC == DT.Citizen
        reOrderAnd :: TestRow DMC.PUMAOuterKeyR DMC.ASER  F.⊆ rs => F.Record rs -> F.Record (TestRow DMC.PUMAOuterKeyR DMC.ASER)
        reOrderAnd = F.rcast @(TestRow DMC.PUMAOuterKeyR DMC.ASER)
        caserToASER :: (FSI.RecVec rs, F.ElemOf rs DT.CitizenC, TestRow DMC.PUMAOuterKeyR DMC.ASER  F.⊆ rs)
                    => F.FrameRec rs -> F.FrameRec (TestRow DMC.PUMAOuterKeyR DMC.ASER)
        caserToASER = fmap reOrderAnd . F.filterFrame isCitizen
    pumaActual <- K.ignoreCacheTime $  fmap caserToASER byPUMA_C
    pumaModeled <- K.ignoreCacheTime $ fmap caserToASER pumaModeled_ASCRE_C
    pumaProduct <- K.ignoreCacheTime $ fmap caserToASER pumaProduct_ASCRE_C


    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        cdKey ok = (ok ^. GT.stateAbbreviation, ok ^. GT.congressionalDistrict)

        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
--        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testCDs
--        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
    pumaModelPaths <- postPaths "Model_CSR_ASR_ASE_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_CSR_ASR_ASE_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASER)
        pumaModelPaths postInfo True False "CSR_ASR_ASE" ("PUMA", pumaPopMap, pumaKey) (Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error (%)" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaActual)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaActual) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast pumaProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast pumaModeled) (Just "NS Model") (Just "NS Model (L2)") (Just "NS Model")
        ]

-}
compareCSR_ASR :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareCSR_ASR cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for CSR x ASR -> CASR"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) = ACS.acs1Yr2010_20
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASR . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C
    testCDs_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.CASR . fmap F.rcast)
                 <$> DDP.cachedACSa5ByCD srcWindow cachedSrc 2020 Nothing
    testCDs <- K.ignoreCacheTime testCDs_C

--    byPUMA <- K.ignoreCacheTime byPUMA_C
    pumaTest_C <- testNS @DMC.PUMAOuterKeyR @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                  (DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull)
                  (Right "CSR_ASR_ByPUMA")
                  (Right "model/demographic/csr_asr_PUMA")
                  (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                  (show . view GT.pUMA) cmdLine Nothing Nothing Nothing byPUMA_C testPUMAs_C
    (pumaProduct, pumaModeled) <- K.ignoreCacheTime pumaTest_C

{-
    (product_CSR_ASR', modeled_CSR_ASR') <- K.ignoreCacheTimeM
                                            $ testProductNS_CD' @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                                            DTP.viaNearestOnSimplex False True "model/demographic/csr_asr" "CSR_ASR" cmdLine byPUMA_C byCD_C
-}
    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        pumaKey ok = (ok ^. GT.stateAbbreviation, ok ^. GT.pUMA)
        cdKey ok = (ok ^. GT.stateAbbreviation, ok ^. GT.congressionalDistrict)

        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testCDs
--        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.citizenC, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.race5C)
    pumaModelPaths <- postPaths "Model_CSR_ASR_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_CSR_ASR_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.CASR)
        pumaModelPaths postInfo True False "CSR_ASR" ("PUMA", pumaPopMap, pumaKey) (Just "NY")
        (F.rcast @DMC.CASR)
        (\r -> (r ^. DT.citizenC, r ^. DT.race5C))
        (\r -> (r ^. DT.sexC, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error (%)" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.CASR testPUMAs)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.CASR testPUMAs) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast pumaProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast pumaModeled) (Just "NS Model") (Just "NS Model (L2)") (Just "NS Model")
        ]

    cdFromPUMAModeled_C <- DDP.cachedPUMAsToCDs @DMC.CASR "model/demographic/csr_asr_CDFromPUMA.bin" $ fmap snd pumaTest_C
    cdFromPUMAModeled <- K.ignoreCacheTime cdFromPUMAModeled_C

    (cdProduct, cdModeled) <- K.ignoreCacheTimeM
                              $ testNS @DMC.CDOuterKeyR @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                              (DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull)
                              (Right "CSR_ASR_ByPUMA")
                              (Right "model/demographic/csr_asr_PUMA")
                              (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                              DDP.districtKeyT cmdLine Nothing Nothing Nothing byPUMA_C testCDs_C

    cdModelPaths <- postPaths "Model_CSR_ASR_ByCD" cmdLine
    BRK.brNewPost cdModelPaths postInfo "Model_CSR_ASR_ByCD" $ do
      compareResults @(DMC.CDOuterKeyR V.++ DMC.CASR)
        pumaModelPaths postInfo True False "CSR_ASR" ("CD", cdPopMap, DDP.districtKeyT) (Just "NY")
        (F.rcast @DMC.CASR)
        (\r -> (r ^. DT.citizenC, r ^. DT.race5C))
        (\r -> (r ^. DT.sexC, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error (%)" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.CDOuterKeyR @DMC.CASR testCDs)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.CDOuterKeyR @DMC.CASR testCDs) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast cdProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast cdModeled) (Just "Direct NS Model (direct)") (Just "Direct NS Model (L2)") (Just "Direct NS Model")
        ,MethodResult (fmap F.rcast cdFromPUMAModeled) (Just "Aggregated NS Model (direct)") (Just "Aggregated NS Model (L2)") (Just "Aggregated NS Model")
        ]
    pure ()

{-
aseASR_ASER_Products :: Text
                     -> K.ActionWithCacheTime r (F.FrameRec (TestRow [GT.StateAbbreviation, GT.PUMA] DMC.ASER))
                     -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (TestRow [GT.StateAbbreviation, GT.PUMA] DMC.ASER)))
aseASR_ASER_Products ck test_C = do
  let testAs_C = (aggregateAndZeroFillTables @[GT.StateAbbreviation, GT.PUMA] @(F.RDeleteAll '[DT.Education4C] DMC.ASER) . fmap F.rcast) <$> test_C
      testBs_C = (aggregateAndZeroFillTables @[GT.StateAbbreviation, GT.PUMA] @(F.RDeleteAll '[DT.Race5C] DMC.ASER) . fmap F.rcast) <$> test_C
  DMC.tableProducts @[GT.StateAbbreviation, GT.PUMA] @DMC.AS @[DT.Race5C] @[DT.Education4C] ck testAs_C testBs_C
-}
-- outerKey cols in input order, innerKey rows in key order
vecNotNaN :: VS.Vector Double -> Bool
vecNotNaN = isNothing . VS.findIndex isNaN

labeledToMatFld :: (Ord outerKey, Ord innerKey, Keyed.FiniteSet innerKey)
                 => (F.Record rs -> outerKey) -> (F.Record rs -> innerKey) -> (F.Record rs -> Double) -> FL.Fold (F.Record rs) (LA.Matrix Double)
labeledToMatFld ok ik f = fmap (LA.fromColumns . filter vecNotNaN)
                          $ MR.mapReduceFold
                          MR.noUnpack
                          (MR.assign ok (\x -> (ik x, f x)))
                          (MR.ReduceFold $ const $ innerFld)
  where
    vecFld = DED.vecFld getSum (Sum . snd) fst
    normFld = FL.premap snd FL.sum
    innerFld = (\v s -> VS.map (/ s) v) <$> vecFld <*> normFld


labeledToProdMatFld :: (Ord outerKey, Ord innerKey, Keyed.FiniteSet innerKey, Monoid w)
                    => Lens' w Double
                    -> DMS.MarginalStructure w innerKey
                    -> (F.Record rs -> outerKey) -> (F.Record rs -> innerKey) -> (F.Record rs -> w) -> FL.Fold (F.Record rs) (LA.Matrix Double)
labeledToProdMatFld wl ms ok ik f = fmap (LA.fromColumns . filter vecNotNaN)
                                    $ MR.mapReduceFold
                                    MR.noUnpack
                                    (MR.assign ok (\x -> (ik x, f x)))
                                    (MR.ReduceFold $ const vecFld)
  where
    vecFld = fmap (VS.fromList . fmap (view wl) . M.elems . FL.fold DMS.zeroFillSummedMapFld) $ DMS.msProdFld ms
--    normFld = FL.premap (view wl . snd) FL.sum
--    innerFld = (\v s -> VS.map (/ s) v) <$> vecFld <*> normFld

totalDeviation :: LA.Vector Double -> LA.Vector Double -> Double
totalDeviation v1 v2 = (VS.sum $ VS.map abs $ VS.zipWith (-) v1 v2) / 2

totalDeviationM :: LA.Matrix Double -> LA.Matrix Double -> LA.Vector Double
totalDeviationM m1 m2 = VS.fromList $ fmap ((/ 2) . VS.sum . VS.map abs) $ LA.toColumns $ m1 - m2

avgTotalDeviation :: LA.Matrix Double -> LA.Matrix Double -> Double
avgTotalDeviation m1 m2 = FL.fold FL.mean $ VS.toList $ totalDeviationM m1 m2

bias :: LA.Matrix Double -> LA.Matrix Double -> LA.Vector Double
bias x y = VS.fromList $ fmap (FL.fold FL.mean . VS.toList) $ LA.toRows $ x - y

avgAbsBias :: LA.Matrix Double -> LA.Matrix Double -> Double
avgAbsBias x y = FL.fold FL.mean $ VS.toList $ VS.map abs $ bias x y

rmse :: LA.Matrix Double -> LA.Matrix Double -> LA.Vector Double
rmse x y = VS.fromList $ fmap (fmap sqrt (FL.fold (FL.premap (^2) FL.mean) . VS.toList)) $ LA.toRows $ x - y

avgRMSE :: LA.Matrix Double -> LA.Matrix Double -> Double
avgRMSE x y = FL.fold FL.mean $ VS.toList $ VS.map abs $ rmse x y

reportErrors :: K.KnitEffects r => Text -> LA.Matrix Double -> LA.Matrix Double -> K.Sem r ()
reportErrors t m1 m2 = do
  K.logLE K.Info $ t <> " <TotalDeviation> = " <> show (avgTotalDeviation m1 m2)
  K.logLE K.Info $ t <> " <RMSE>           = " <> show (avgRMSE m1 m2)
  K.logLE K.Info $ t <> " <|bias|>         = " <> show (avgAbsBias m1 m2)

addToAll :: Double -> LA.Vector Double -> LA.Vector Double
addToAll x = VS.map (+ x)

jitter :: Int -> LA.Vector Double -> LA.Vector Double
jitter m = addToAll (1 / realToFrac (m * m))

newtype FlatMatrix = FlatMatrix { matrix :: LA.Matrix Double }
instance Flat.Flat FlatMatrix where
  size = Flat.size . LA.toColumns . matrix
  encode = Flat.encode . LA.toColumns . matrix
  decode = FlatMatrix . LA.fromColumns <$> Flat.decode

asrASE_average_stuff :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => K.Sem r ()
asrASE_average_stuff = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2010_20
  testPUMAs_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASER . fmap F.rcast)
                 <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
  (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                     $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) testPUMAs_C
  testPUMAs <- K.ignoreCacheTime testPUMAs_C
  let ms = DMC.marginalStructure @DMC.ASER  @'[DT.Race5C] @'[DT.Education4C] @DMS.CellWithDensity @DMC.AS DMS.cwdWgtLens DMS.innerProductCWD'
  nullVecsSVD <- K.knitMaybe "asrASE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                 $  DTP.nullVecsMS ms Nothing


  let catMapASER :: DNS.CatMap (F.Record DMC.ASER) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                                , ('S', S.size $ Keyed.elements @DT.Sex)
                                                                , ('E', S.size $ Keyed.elements @DT.Education4)
                                                                , ('R', S.size $ Keyed.elements @DT.Race5)
                                                                ]
      catAndKnownASR_ASE :: DNS.CatsAndKnowns (F.Record DMC.ASER) = DNS.CatsAndKnowns catMapASER
                                                                    [DNS.KnownMarginal "ASR", DNS.KnownMarginal "ASE"]
  nullVecsTensor <- K.knitMaybe "aseASE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                    $  DTP.nullVecsMS ms (Just catAndKnownASR_ASE)
  let covFld :: FL.Fold (F.Record (DMC.ASER V.++ '[DT.PopCount, DT.PWPopPerSqMile])) [Double]
      covFld = fmap VS.toList $ DMC.innerFoldWD (F.rcast @DMC.ASR) (F.rcast @DMC.ASE)
      popVecFld = DED.vecFld getSum (Sum . realToFrac . view DT.popCount) (F.rcast @DMC.ASER)
      popFld = FL.premap (realToFrac . view DT.popCount) FL.sum
      safeDiv x y = if y /= 0 then x / y else 0
      alphaFld nv = (\p -> VS.toList . DTP.fullToProj nv . VS.map (flip safeDiv p)) <$> popFld <*> popVecFld
      alphaCovInner nv k = (\as cs -> AlphaCov (k ^. GT.stateAbbreviation) (k ^. GT.pUMA) as cs) <$> alphaFld nv <*> covFld
      alphaCovFld ::  DTP.NullVectorProjections (F.Record DMC.ASER) -> FL.Fold (F.Record (DMC.PUMARowR DMC.ASER)) [AlphaCov]
      alphaCovFld nv = MR.mapReduceFold
                       MR.noUnpack
                       (MR.assign (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @(DMC.ASER V.++ '[DT.PopCount, DT.PWPopPerSqMile])))
                       (MR.ReduceFold $ alphaCovInner nv)
      alphaCovs nv = FL.fold (alphaCovFld nv) testPUMAs

--  K.liftKnit $ writeAlphaCovs 120 91 "../../forPhilip/alphaCovs.csv" alphaCovs
  let avgAlphaFld n = FL.premap (\ac -> acAlphas ac List.!! n) FL.mean
      avgAlphasFld = fmap VS.fromList $ traverse avgAlphaFld [0..119]
      avgAlphas nv = FL.fold avgAlphasFld $ alphaCovs nv
      avgChange nv = zip (S.toList $ Keyed.elements @(F.Record DMC.ASER)) $ VS.toList $ DTP.projToFull nv $ avgAlphas nv
      showKey k = T.intercalate ", " $ [show (k ^. DT.age5C), show (k ^. DT.sexC), show (k ^. DT.education4C), show (k ^. DT.race5C)]
{-  K.liftKnit @IO
    $ sWriteTextLines "../../forPhilip/change.csv"
    $ FSS.StreamlyStream @FSS.Stream
    $ fmap (\(k, v) -> showKey k <> ", " <> toText @String (PF.printf "%2.2g" v))
    $ FSS.stream $ sFromFoldable @_ @IO avgChange
  let labeledBasis = zip (S.toList $ Keyed.elements @(F.Record DMC.ASER)) $ LA.toRows $ DTP.projToFullM nullVecs
      mBym = LA.tr (DTP.projToFullM nullVecs) LA.<> DTP.projToFullM nullVecs
  K.logLE K.Info $ toText $ LA.dispf 4 mBym
  let vecText = T.intercalate ", " . fmap (toText @String . PF.printf "%2.8g") . VS.toList
      basisRowText (k, v) = showKey k <> ", " <> vecText v
  K.liftKnit @IO
    $ sWriteTextLines "../../forPhilip/basis.csv"
    $ FSS.StreamlyStream @FSS.Stream
    $ fmap basisRowText
    $ FSS.stream $ sFromFoldable @_ @IO labeledBasis
  let labeledConstraints = zip (S.toList $ Keyed.elements @(F.Record DMC.ASER)) $ LA.toRows $ LA.tr $ DTP.nvpConstraints nullVecs
  K.liftKnit @IO
    $ sWriteTextLines "../../forPhilip/constraints.csv"
    $ FSS.StreamlyStream @FSS.Stream
    $ fmap basisRowText
    $ FSS.stream $ sFromFoldable @_ @IO labeledConstraints
-}
  K.logLE K.Info $ "Computing total deviation of products..."
  let pumasToMatFld :: FL.Fold (F.Record (DMC.PUMARowR DMC.ASER)) (LA.Matrix Double)
      pumasToMatFld = labeledToMatFld (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @DMC.ASER) (realToFrac . view DT.popCount)
      pumasToProdMatFld :: FL.Fold (F.Record (DMC.PUMARowR DMC.ASER)) (LA.Matrix Double)
      pumasToProdMatFld = labeledToProdMatFld DMS.cwdWgtLens ms (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @DMC.ASER) DTM3.cwdF
  (pumasTM, pumaProductsTM) <- fmap (first matrix . second matrix)
                               $ K.ignoreCacheTimeM
                               $ BRCC.retrieveOrMakeD "test/ASR_ASE/pumaMatricesTesting.bin" pumaTesting_C $ \x -> do
    K.logLE K.Info "(Re)building Testing PUMA data"
    let (pumas, products) = FL.fold ((,) <$> pumasToMatFld <*> pumasToProdMatFld) x
    pure $ (FlatMatrix pumas, FlatMatrix products)
  (pumasTrM, pumaProductsTrM) <- fmap (first matrix . second matrix)
                               $ K.ignoreCacheTimeM
                               $ BRCC.retrieveOrMakeD "test/ASR_ASE/pumaMatricesTraining.bin" pumaTraining_C $ \x -> do
    K.logLE K.Info "(Re)building Training PUMA data"
    let (pumas, products) = FL.fold ((,) <$> pumasToMatFld <*> pumasToProdMatFld) x
    pure $ (FlatMatrix pumas, FlatMatrix products)

--  K.logLE K.Info $ toText $ LA.dispf 4 pumasM
--  K.logLE K.Info $ toText $ LA.dispf 4 pumaProductsM

--      pumaProductsM = FL.fold pumasToProdMatFld testPUMAs
  let totalDeviations x = zipWith totalDeviation (LA.toColumns pumasTM) (LA.toColumns x)
      avgDeviation x = FL.fold FL.mean $ totalDeviations x
      nPUMA = LA.cols pumasTM
  K.logLE K.Info $ "N_puma=" <> show nPUMA
  K.logLE K.Info $ "Product: avg deviation=" <> show (avgDeviation pumaProductsTM)
  let diffs = pumasTrM - pumaProductsTrM
      alphas nv = DTP.fullToProjM nv LA.<> diffs
      (meanAlphaSVD, covAlphaSVD) = LA.meanCov $ LA.tr $ alphas nullVecsSVD
      (meanAlphaTensor, covAlphaTensor) = LA.meanCov $ LA.tr $ alphas nullVecsTensor

  K.logLE K.Info $ "SVD alphas=" <> DED.prettyVector meanAlphaSVD
  K.logLE K.Info $ "Tensor alphas=" <> DED.prettyVector meanAlphaTensor
--  K.logLE K.Info $ "alpha covs=" <> (toText $ LA.disps 4 $ LA.unSym covAlpha)
  let prodAvgAlphaSVD = pumaProductsTM + LA.fromColumns (replicate nPUMA $ DTP.projToFullM nullVecsSVD LA.#> meanAlphaSVD)
--      aaTotalDeviations = zipWith totalDeviation (LA.toColumns pumasTM) (LA.toColumns prodAvgAlpha)
--      avgAADeviation = FL.fold FL.mean aaTotalDeviations
  K.logLE K.Info $ "SVD Mean alpha: avg deviation=" <> show (avgDeviation prodAvgAlphaSVD) --avgAADeviation
  let prodAvgAlphaTensor = pumaProductsTM + LA.fromColumns (replicate nPUMA $ DTP.projToFullM nullVecsTensor LA.#> meanAlphaTensor)
  K.logLE K.Info $ "Tensor Mean alpha: avg deviation=" <> show (avgDeviation prodAvgAlphaTensor) --avgAADeviation

  let aaOSSVD = LA.fromColumns $ fmap DTP.projectToSimplex $ LA.toColumns prodAvgAlphaSVD
      aaOSTensor = LA.fromColumns $ fmap DTP.projectToSimplex $ LA.toColumns prodAvgAlphaTensor
--      aaOsTotalDeviations = zipWith totalDeviation (LA.toColumns pumasTM) (LA.toColumns aaOS)
--      avgAAOSDeviation = FL.fold FL.mean aaOsTotalDeviations
  K.logLE K.Info $ "SVD: avg AAOS deviation=" <> show (avgDeviation aaOSSVD)
  K.logLE K.Info $ "Tensor: avg AAOS deviation=" <> show (avgDeviation aaOSTensor)
  K.logLE K.Info $ "Weight vector optimization via NNLS"
  let toJoint nv alphas = pumaProductsTM + DTP.projToFullM nv LA.<> alphas
      owOptimizerDataSVD = zip (LA.toColumns pumaProductsTM) (replicate nPUMA meanAlphaSVD)
      nnlsLSI_E nV = LH.precomputeFromE (DTP.projToFullM nV)
      leftError me = do
        e <- me
        case e of
          Left msg -> K.knitError msg
          Right x -> pure x
      asOptimize nV mLSI_E (pV, aV) = DED.mapPE $ DTP.optimalWeightsAS DTP.semThrow Nothing mLSI_E nV aV pV
--  aaASTensor <- LA.fromColumns <$> (traverse (asOptimize nullVecsTensor) $ owOptimizerData meanAlphaTensor)
  aaASSVD <- logTime "NNLS" (LA.fromColumns <$> (traverse (asOptimize nullVecsSVD Nothing) $ owOptimizerDataSVD))
  K.logLE K.Info $ "avg SVD AAAS deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaASSVD)
  aaASSVD <- logTime "NNLS (precompute)" (LA.fromColumns <$> (traverse (asOptimize nullVecsSVD (Just $ nnlsLSI_E nullVecsSVD)) $ owOptimizerDataSVD))
  K.logLE K.Info $ "avg SVD NNLS (precompute) deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaASSVD)


--  K.logLE K.Info $ "avg Tensor AAAS deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsTensor aaASTensor)

{-  K.logLE K.Info $ "doing full vector optimization"
  let optimizerDataSVD = zip (LA.toColumns pumaProductsTM) (LA.toColumns aaOSSVD)
      optimizerDataTensor = zip (LA.toColumns pumaProductsTM) (LA.toColumns aaOSTensor)
      optimize nv (p, t) = DED.mapPE $ DTP.optimalVector nv p t
  aaOVSVD <- LA.fromColumns <$> traverse (optimize nullVecsSVD) optimizerDataSVD
  aaOVTensor <- LA.fromColumns <$> traverse (optimize nullVecsTensor) optimizerDataTensor
  K.logLE K.Info $ "SVD: avg AAOV deviation=" <> show (avgDeviation aaOVSVD)
  K.logLE K.Info $ "Tensor: avg AAOV deviation=" <> show (avgDeviation aaOVTensor)
-}
  K.logLE K.Info $ "doing weight vector optimization"
  let owOptimizerDataTensor = zip (LA.toColumns pumaProductsTM) (replicate nPUMA meanAlphaTensor)
      owOptimize nv (pV, aV) = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull nv aV pV
  aaOWSVD' <- logTime "SLSQP" (LA.fromColumns <$> traverse (owOptimize nullVecsSVD) owOptimizerDataSVD)
--  aaOWTensor' <- LA.fromColumns <$> traverse (owOptimize nullVecsTensor) owOptimizerDataTensor
  let aaOWSVD = pumaProductsTM + DTP.projToFullM nullVecsSVD LA.<> aaOWSVD'
--  let aaOWTensor = pumaProductsTM + DTP.projToFullM nullVecsTensor LA.<> aaOWTensor'
  K.logLE K.Info $ "SVD: avg AAOW deviation=" <> show (avgDeviation aaOWSVD)
--  K.logLE K.Info $ "Tensor: avg AAOW deviation=" <> show (avgDeviation aaOWTensor)

  K.logLE K.Info $ "doing weight vector optimization, invWeight"
  let iwOptimize f nV (pV, aV) = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) (DTP.euclideanWeighted f) nV aV pV
      tryF t f = do
        aaIWSVD <- logTime "weighted SLSQP" (LA.fromColumns <$> (traverse (iwOptimize f nullVecsSVD) owOptimizerDataSVD))
        K.logLE K.Info $ "avg SVD AAIW deviation (f is " <> t <> ")=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaIWSVD)
--  tryF "-log x" (negate . Numeric.log)
--  tryF "1/x" (\x -> 1/x)
--  tryF "x^(-0.75)" (\x -> x ** (-0.75))
  tryF "1/sqrt(x)" (\x -> 1/sqrt x)
--  tryF "x^(-0.25)" (\x -> x ** (-0.25))

--      totalDev
  pure ()
--      allPUMAMat = FL.fold pumasToMatFld testPUMAs

type TractGeoR = [BRDF.Year, GT.StateAbbreviation, GT.TractGeoId]
type TractRow ks = ACS.CensusRow ACS.TractLocationR ACS.CensusDataR ks

cachedAsMatrix :: forall ls ks rs r .
                   (K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r
                   , F.ElemOf rs DT.PopCount
                   , F.ElemOf rs DT.PWPopPerSqMile
                   , ks F.⊆ rs, ls F.⊆ rs
                   , Ord (F.Record ls)
                   , Ord (F.Record ks)
                   , Keyed.FiniteSet (F.Record ks)
                   )
                => Text
                -> Text
                -> K.ActionWithCacheTime r (F.FrameRec rs)
                -> K.Sem r (K.ActionWithCacheTime r (LA.Matrix Double))
cachedAsMatrix cachePrefix cacheLabel rows_C = do
  let tractsToMatFld :: FL.Fold (F.Record rs) (LA.Matrix Double)
      tractsToMatFld = labeledToMatFld (F.rcast @ls) (F.rcast @ks) (realToFrac . view DT.popCount)
  logTime (cacheLabel <> " -> to cached matrix")
    $ fmap (fmap matrix)
    $ BRCC.retrieveOrMakeD (cachePrefix <> cacheLabel <> ".bin") rows_C $ \x -> do
        let m = FL.fold tractsToMatFld x --fmap F.rcast x
        pure $ FlatMatrix m

cachedFullAndProductC :: forall ls ks rs r .
                        (K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r
                        , F.ElemOf rs DT.PopCount
                        , F.ElemOf rs DT.PWPopPerSqMile
                        , ks F.⊆ rs, ls F.⊆ rs
                        , Ord (F.Record ls)
                        , Ord (F.Record ks)
                        , Keyed.FiniteSet (F.Record ks)
                        )
                     => DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                     -> Text
                     -> Text
                     -> K.ActionWithCacheTime r (F.FrameRec rs)
                     -> K.Sem r (K.ActionWithCacheTime r (LA.Matrix Double, LA.Matrix Double))
cachedFullAndProductC ms cachePrefix cacheLabel rows_C = do
  let tractsToMatFld :: FL.Fold (F.Record rs) (LA.Matrix Double)
      tractsToMatFld = labeledToMatFld (F.rcast @ls) (F.rcast @ks) (realToFrac . view DT.popCount)
      tractsToProdMatFld :: FL.Fold (F.Record rs) (LA.Matrix Double)
      tractsToProdMatFld = labeledToProdMatFld DMS.cwdWgtLens ms (F.rcast @ls) (F.rcast @ks) DTM3.cwdF
  logTime (cacheLabel <> " -> matrices & product matrices")
    $ fmap (fmap (first matrix . second matrix))
    $ BRCC.retrieveOrMakeD (cachePrefix <> "matrices" <> cacheLabel <> ".bin") rows_C $ \x -> do
        let (full, products) = FL.fold ((,) <$> tractsToMatFld <*> tractsToProdMatFld) $ x --fmap F.rcast x
        pure $ (FlatMatrix full, FlatMatrix products)


cachedFullAndProduct :: forall ls ks rs r .
                        (K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r
                        , F.ElemOf rs DT.PopCount
                        , F.ElemOf rs DT.PWPopPerSqMile
                        , ks F.⊆ rs, ls F.⊆ rs
                        , Ord (F.Record ls)
                        , Ord (F.Record ks)
                        , Keyed.FiniteSet (F.Record ks)
                        )
                     => DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                     -> Text
                     -> Text
                     -> K.ActionWithCacheTime r (F.FrameRec rs)
                     -> K.Sem r (LA.Matrix Double, LA.Matrix Double)
cachedFullAndProduct ms cachePrefix cacheLabel rows_C = cachedFullAndProductC @ls ms cachePrefix cacheLabel rows_C >>= K.ignoreCacheTime

buildCachedTestTrain :: forall ls ks rs a b r .
                        (K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r
                        , F.ElemOf rs DT.PopCount
                        , F.ElemOf rs DT.PWPopPerSqMile
                        , ks F.⊆ rs, ls F.⊆ rs
                        , Ord (F.Record ls)
                        , Ord (F.Record ks)
                        , Keyed.FiniteSet (F.Record ks)
                        , Eq a, Eq b
                        , FSI.RecVec rs)
                     => (F.Record rs -> a)
                     -> (F.Record rs -> b)
                     -> DMS.MarginalStructure DMS.CellWithDensity (F.Record ks)
                     -> Text
                     -> K.ActionWithCacheTime r (F.FrameRec rs)
                     -> K.Sem r (LA.Matrix Double, LA.Matrix Double, LA.Matrix Double, LA.Matrix Double)
buildCachedTestTrain splitRegion splitId ms cachePrefix rows_C = do
  (training_C, testing_C) <- logTime "Training/testing split"
                             $ KC.wctSplit
                             $ KC.wctBind (splitGEOs testHalf splitRegion splitId) rows_C

  (testM, testPM) <- cachedFullAndProduct @ls ms cachePrefix "Test" testing_C --matrices "Test" testing_C
  (trainM, trainPM) <- cachedFullAndProduct @ls ms cachePrefix "Train" training_C --matrices "Train" training_C
  pure (testM, testPM, trainM, trainPM)

products :: forall a b ok ika ikb k. (BRK.FiniteSet a, Ord a, BRK.FiniteSet b, Ord b,
                                       BRK.FiniteSet ok, Ord k, BRK.FiniteSet ika, BRK.FiniteSet ikb, Ord ok, Ord ika, Ord ikb)
         => (a -> (ok, ika)) -> (b -> (ok, ikb)) -> ((ok, ika, ikb) -> k) -> LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double
products aKeys bKeys toK aM bM = LA.fromColumns $ zipWith f (LA.toColumns aM) (LA.toColumns bM) where
  toMap :: forall c ikc . (BRK.FiniteSet c, Ord c, BRK.FiniteSet ikc, Ord ikc) => (c -> (ok, ikc)) -> LA.Vector Double -> Map ok (Map ikc (Sum Double))
  toMap cKeys v = FL.fold DMS.tableMapFld $ zip (fmap cKeys $ S.toList $ BRK.elements @c) (fmap Sum $ VS.toList v)
  f a b = VS.fromList $ fmap (getSum . snd) $ sortOn fst $ fmap (first toK) $ DMS.tableProductL DMS.innerProductSum (toMap aKeys a) (toMap bKeys b)

-- covariates to alphas
type AlphaModel m = LA.Vector Double -> m (LA.Vector Double)

averageAlphaModel :: Applicative m => LA.Matrix Double -> AlphaModel m
averageAlphaModel rawAlphaM = const $ pure mean
  where (mean, _) = LA.meanCov $ LA.tr rawAlphaM

-- NB: our inputs are one column per geography but
-- W'W \beta = W'A is written for the transposes of the
alphaProductLR :: Applicative m => LA.Matrix Double -> LA.Matrix Double -> AlphaModel m
alphaProductLR pM aM  = pure . (beta' LA.#>)
  where
    w = LA.tr pM
    wTw = LA.mTm w
    wChol = LA.chol wTw
    beta' = LA.tr $ LA.cholSolve wChol $ pM LA.<> LA.tr aM

-- general function to do toCols . traverse f . fromCols using streamly
applyToCols :: (VS.Storable a, LA.Element a, Applicative m) => (VS.Vector a -> m (VS.Vector a)) -> LA.Matrix a -> m (LA.Matrix a)
applyToCols f = fmap LA.fromColumns . traverse f . LA.toColumns


streamlyApplyToCols :: (Streamly.MonadAsync m, LA.Element a, VS.Storable a)
                    => (Streamly.Config -> Streamly.Config) -> (VS.Vector a -> m (VS.Vector a)) -> LA.Matrix a -> m (LA.Matrix a)
streamlyApplyToCols cf f = fmap LA.fromColumns . Streamly.toList . Streamly.parMapM cf f . Streamly.fromList . LA.toColumns


-- matrix of covariates to matrix of alphas
applyAlphaModel :: Applicative m => AlphaModel m -> LA.Matrix Double -> m (LA.Matrix Double)
applyAlphaModel = applyToCols

trueAlpha :: DTP.NullVectorProjections k -> LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double
trueAlpha nv true prod = DTP.fullToProjM nv LA.<> diffs
  where diffs = true - prod

type OnSimplex m = LA.Vector Double -> LA.Vector Double -> m (LA.Vector Double)

applyOnSimplex :: Applicative  m => OnSimplex m -> LA.Matrix Double -> LA.Matrix Double -> m (LA.Matrix Double)
applyOnSimplex os pM aM = fmap LA.fromColumns $ traverse (uncurry os) $ zip (LA.toColumns pM) (LA.toColumns aM)

applyOnSimplexP :: Streamly.MonadAsync m =>  (Streamly.Config -> Streamly.Config) -> OnSimplex m -> LA.Matrix Double -> LA.Matrix Double -> m (LA.Matrix Double)
applyOnSimplexP cf os pM aM =  fmap LA.fromColumns $ Streamly.toList $ Streamly.parMapM cf (uncurry os) $ Streamly.fromList $ zip (LA.toColumns pM) (LA.toColumns aM)

noOnSimplex :: Applicative m => OnSimplex m
noOnSimplex _ aM = pure aM

nearestOnSimplex :: Applicative m => DTP.NullVectorProjections k  -> OnSimplex m
nearestOnSimplex nvps pM aM = pure $ DTP.fullToProj nvps $ DTP.projectToSimplex (pM + DTP.projToFull nvps aM)

euclideanSLSQP :: K.KnitEffects r => DTP.NullVectorProjections k -> OnSimplex (K.Sem r)
euclideanSLSQP nvps pM aM = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull nvps aM pM

nnls :: MonadIO m => (forall a. Text -> m a) -> DTP.NullVectorProjections k -> OnSimplex m
nnls throw nvps pV aV =
  let mLSIE = Just $ LH.precomputeFromE $ DTP.projToFullM nvps
  in DTP.optimalWeightsAS throw Nothing mLSIE nvps aV pV

weightedSLSQP :: K.KnitEffects r => DTP.NullVectorProjections k -> (Double -> Double) -> OnSimplex (K.Sem r)
weightedSLSQP nvps f pM aM = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) (DTP.euclideanWeighted f) nvps aM pM

modelAndSimplex :: Monad m => AlphaModel m -> OnSimplex m -> LA.Matrix Double -> LA.Matrix Double -> m (LA.Matrix Double)
modelAndSimplex am os covM pM = applyToCols am covM >>= \aM -> applyOnSimplex os pM aM

modelAndSimplexP :: Streamly.MonadAsync m
                 => (Streamly.Config -> Streamly.Config)
                 -> AlphaModel m -> OnSimplex m -> LA.Matrix Double -> LA.Matrix Double -> m (LA.Matrix Double)
modelAndSimplexP cf am os covM pM = streamlyApplyToCols cf am covM >>= \aM -> applyOnSimplexP cf os pM aM

modelSimplexEstimate :: Monad m => DTP.NullVectorProjections k -> AlphaModel m -> OnSimplex m -> (LA.Vector Double, LA.Vector Double) -> m (LA.Vector Double)
modelSimplexEstimate nvps am os (covV, pV) = do
  aV <- am covV
  osaV <- os pV aV
  pure $ pV + DTP.projToFull nvps osaV

modelSimplexEstimatesP :: Streamly.MonadAsync m
                       => (Streamly.Config -> Streamly.Config)
                       -> DTP.NullVectorProjections k
                       -> AlphaModel m
                       -> OnSimplex m
                       -> LA.Matrix Double
                       -> LA.Matrix Double
                       -> m (LA.Matrix Double)
modelSimplexEstimatesP cf nvps am os covM pM =
  fmap LA.fromColumns
  $ Streamly.toList
  $ Streamly.parMapM cf (modelSimplexEstimate nvps am os)
  $ Streamly.fromList
  $ zip (LA.toColumns covM) (LA.toColumns pM)

estimates :: DTP.NullVectorProjections k -> LA.Matrix Double -> LA.Matrix Double -> LA.Matrix Double
estimates nvps pM aM = pM + DTP.projToFullM nvps LA.<> aM

estimate :: Monad m => DTP.NullVectorProjections k -> AlphaModel m -> OnSimplex m -> LA.Matrix Double -> LA.Matrix Double -> m (LA.Matrix Double)
estimate nvps am os covM pM = fmap (estimates nvps pM) $ modelAndSimplex am os covM pM

asAE_Tracts :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => K.Sem r ()
asAE_Tracts = do
  K.logLE K.Info "Tracts"
  tractTables_C <- logTime "Load Tables" $ ACS.loadACS_2017_2022_Tracts
  allTractsASE_C <- logTime "Aggregate..."
                    $ BRCC.retrieveOrMakeFrame "test/demographics/allTractsASE.bin" tractTables_C
                    $ pure . (aggregateAndZeroFillTables @TractGeoR @DMC.ASE . fmap F.rcast . ACS.ageSexEducation)
  tractIds <-  logTime "Ids to list (load cached data)" (FL.fold (FL.premap (view GT.tractGeoId) FL.list) <$> K.ignoreCacheTime allTractsASE_C)
  let n = FL.fold FL.length tractIds
  K.logLE K.Info $  "Loaded " <> show n <> " rows; " <> show (n `div` 40) <> " tracts."
  let ms = DMC.marginalStructure @DMC.ASE  @'[DT.SexC] @'[DT.Education4C] @DMS.CellWithDensity @'[DT.Age5C] DMS.cwdWgtLens DMS.innerProductCWD
  nullVecsSVD <- K.knitMaybe "asAE_Tracts_Avg: Problem constructing SVD nullVecs. Bad marginal subsets?"
                 $  DTP.nullVecsMS ms Nothing

  (te, teP, tr, trP) <- buildCachedTestTrain @[GT.StateAbbreviation, GT.TractGeoId] (view GT.stateAbbreviation) (view GT.tractGeoId) ms "test/demographics/Tracts_AS_AE/" allTractsASE_C
  reportVariations nullVecsSVD (te, teP) (tr, trP)



csrASR_ASER_Tracts :: forall r . (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => Int -> K.Sem r ()
csrASR_ASER_Tracts n = K.wrapPrefix "csrASR_Tracts" $ do
  let cacheDir =  "test/demographics/tractsCSR_ASR_ASE/"
  K.logLE K.Info "Loading PUMAs as reference geography"
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
  puma_C <-  DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
  pumaCASR_C <- logTime "Full PUMA data to zero-filled SRCA"
                $ BRCC.retrieveOrMakeFrame  (cacheDir <> "pumaSRCA.bin") puma_C
                $ pure . aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASR . fmap F.rcast
  let msCASR  = DMC.marginalStructure @DMC.CASR  @'[DT.CitizenC] @'[DT.Age5C] @DMS.CellWithDensity @[DT.SexC, DT.Race5C] DMS.cwdWgtLens DMS.innerProductCWD
  nullVecsCASR <-  logTime "CASR NullVecs"
                   $ K.knitMaybe "nullVecsCASR: Problem constructing nullVecs. Bad marginal subsets?"
                   $ DTP.nullVecsMS msCASR Nothing
  pumaCASRFullAndProd_C <- cachedFullAndProductC @[GT.StateAbbreviation, GT.PUMA] msCASR cacheDir "PumaCASRFullProd" pumaCASR_C
  pumaCASRAlphas_C <- logTime "pumaCASR_alphas"
                      $ fmap (fmap matrix)
                      $ BRCC.retrieveOrMakeD (cacheDir <> "pumaCASR_alphas.bin") pumaCASRFullAndProd_C
                      $ \ (f, p) -> pure $ FlatMatrix $ trueAlpha nullVecsCASR f p

  tractTables_C <- logTime "Load Tract Tables" $ ACS.loadACS_2017_2022_Tracts
  tractsCSR_C <- cachedAsMatrix @TractGeoR @DMC.CSR cacheDir "tractCSRMatrix"
                 $ fmap (DMC.recodeCSR @TractGeoR . fmap F.rcast)
                 $ fmap ACS.citizenshipSexRace tractTables_C
  tractsASR_C <- cachedAsMatrix @TractGeoR @DMC.ASR cacheDir "tractASRMatrix"
                 $ fmap (DMC.filterA6FrameToA5 .  DMC.recodeA6SR @TractGeoR . fmap F.rcast)
                 $ fmap ACS.ageSexRace tractTables_C
  tractsSER_C <- cachedAsMatrix @TractGeoR @DMC.SER cacheDir "tractSERMatrix"
                 $ fmap (DMC.recodeSER @TractGeoR . fmap F.rcast)
                 $ fmap ACS.sexEducationRace tractTables_C
  let csrKeys :: F.Record DMC.CSR -> (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.CitizenC])
      csrKeys k = (F.rcast @'[DT.SexC, DT.Race5C] k, F.rcast @'[DT.CitizenC] k)
      asrKeys :: F.Record DMC.ASR -> (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.Age5C])
      asrKeys k = (F.rcast @'[DT.SexC, DT.Race5C] k, F.rcast @'[DT.Age5C] k)
      serKeys :: F.Record DMC.SER -> (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.Education4C])
      serKeys k = (F.rcast @'[DT.SexC, DT.Race5C] k, F.rcast @'[DT.Education4C] k)
      toCASR :: (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.CitizenC], F.Record '[DT.Age5C]) -> F.Record DMC.CASR
      toCASR (sr, c, a) = F.rcast @DMC.CASR (sr F.<+> c F.<+> a)
      csrASR_prodDeps = (,) <$> tractsCSR_C <*> tractsASR_C
  tractProd_CASR_C <- logTime "make tract products"
                      $ fmap (fmap matrix)
                      $ BRCC.retrieveOrMakeD (cacheDir  <> "csrASR_tractProducts.bin") csrASR_prodDeps
                      $ \(csr, asr) -> pure $ FlatMatrix $ products csrKeys asrKeys toCASR csr asr
  numTracts <- logTime "counting tracts" $ (LA.cols <$> K.ignoreCacheTime tractsCSR_C)
  testIndices <- randomIndices' numTracts n
  let testM m = m LA.?? (LA.All, LA.Pos $ LA.idxs testIndices)
      predictAvgSLSQP t nv (pumaAlphas, tractProducts) =
        logTime ("predict " <> t <> " (SLSQP)") $ estimate nv (averageAlphaModel pumaAlphas) (euclideanSLSQP nv) tractProducts tractProducts
  let tractsCASR_C = K.wctBind (predictAvgSLSQP "CASR" nullVecsCASR) $ (,) <$> pumaCASRAlphas_C <*> fmap testM tractProd_CASR_C


  let nnlsConfig = DTP.defaultActiveSetConfig { DTP.cfgLogging = DTP.LogOnError, DTP.cfgStart = DTP.StartZero, DTP.cfgSolver = DTP.SolveLS}
      lsiE_CASR = LH.precomputeFromE (DTP.projToFullM nullVecsCASR)

  let predictNNLS t nullVecs (pumaAlphas, tractProducts) =
        let lsiE = LH.precomputeFromE (DTP.projToFullM nullVecs)
        in logTime ("Predict " <> t <> " " <> show n <> " NNLS")
           $ estimate nullVecs (averageAlphaModel pumaAlphas) (nnls DTP.semThrow nullVecs) tractProducts tractProducts
  let nnlsCASR_C = K.wctBind (DED.mapPE . predictNNLS "CASR" nullVecsCASR) $ (,) <$> pumaCASRAlphas_C <*> tractProd_CASR_C


  let casrKeys :: F.Record DMC.CASR -> (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.CitizenC, DT.Age5C])
      casrKeys k = (F.rcast @[DT.SexC, DT.Race5C] k, F.rcast @[DT.CitizenC, DT.Age5C] k)

      toCASER :: (F.Record [DT.SexC, DT.Race5C], F.Record '[DT.CitizenC, DT.Age5C], F.Record '[DT.Education4C]) -> F.Record DMC.CASER
      toCASER (sr, ca, e) = F.rcast @DMC.CASER (sr F.<+> ca F.<+> e)
  pumaCASER_C <- logTime "Full PUMA data to zero-filled CASER"
                 $ BRCC.retrieveOrMakeFrame  (cacheDir <> "pumaCASER.bin") puma_C
                 $ pure . aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASER . fmap F.rcast
  let msCASER = DMC.marginalStructure @DMC.CASER  @'[DT.CitizenC, DT.Age5C] @'[DT.Education4C] @DMS.CellWithDensity @[DT.SexC, DT.Race5C] DMS.cwdWgtLens DMS.innerProductCWD
  nullVecsCASER <-  logTime "CASER Null Vecs"
                    $ K.knitMaybe "csrASR_ASER_Tracts: Problem constructing CASER nullVecs. Bad marginal subsets?"
                    $ DTP.nullVecsMS msCASER Nothing
  pumaCASERFullAndProd_C <- cachedFullAndProductC @[GT.StateAbbreviation, GT.PUMA] msCASER cacheDir "PumaCASERFullProd" pumaCASER_C
  pumaCASERAlphas_C <- logTime "pumaCASER_alphas"
                       $ fmap (fmap matrix)
                       $ BRCC.retrieveOrMakeD (cacheDir <> "pumaCASER_alphas.bin") pumaCASERFullAndProd_C
                       $ \ (f, p) -> pure $ FlatMatrix $ trueAlpha nullVecsCASER f p
  let casrSER_prodDeps = (,) <$> tractsCASR_C <*> fmap testM tractsSER_C -- because the CASR set is already reduced
      tractProd_CASER_C =  K.wctBind (\(casr, ser) ->  logTime "make tract CASER products" $ pure $ products casrKeys serKeys toCASER casr ser) casrSER_prodDeps
      tractsCASER_C = K.wctBind (predictAvgSLSQP "CASER" nullVecsCASER) $ (,) <$> pumaCASERAlphas_C <*> tractProd_CASER_C
--  slsqpCASERCount <- LA.cols <$> K.ignoreCacheTime tractsCASER_C
--  K.logLE K.Info $ "Final matrix has " <> show slsqpCASERCount <> " cols"

  let parallelPipeline t nv pumaAlphaM productM =
        logTime ("Predict " <> show n <> " " <> show t <> " via avg/NNLS (Parallel via Streamly)")
        $ KS.streamlyToKnit
        $ modelSimplexEstimatesP
        (Streamly.ordered True)
        nv (averageAlphaModel pumaAlphaM) (nnls KS.errStreamly nv) productM productM


  let nnlsCASER_C = K.wctBind (DED.mapPE . predictNNLS "CASER" nullVecsCASER) $ (,) <$> pumaCASERAlphas_C <*> tractProd_CASER_C
  nnlsCASERCount <- LA.cols <$> K.ignoreCacheTime nnlsCASER_C
  K.logLE K.Info $ "Final nnls matrix has " <> show nnlsCASERCount <> " cols"

  let nnlsCASER_P_C = K.wctBind (uncurry $ parallelPipeline "CASER" nullVecsCASER) $ (,) <$> pumaCASERAlphas_C <*> tractProd_CASER_C
  nnlsCASERCountP <- LA.cols <$> K.ignoreCacheTime nnlsCASER_P_C
  K.logLE K.Info $ "Final parallel nnls matrix has " <> show nnlsCASERCountP <> " cols"

  pure ()

asAE_PUMAs :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => K.Sem r ()
asAE_PUMAs = do
  K.logLE K.Info "PUMAs"
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
  allPUMAs_C <- logTime "PUMA data to cache"
                 $ fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASE . fmap F.rcast)
                 <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
  let ms = DMC.marginalStructure @DMC.ASE  @'[DT.SexC] @'[DT.Education4C] @DMS.CellWithDensity @'[DT.Age5C] DMS.cwdWgtLens DMS.innerProductCWD'
  nullVecsSVD <- K.knitMaybe "asAE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                 $  DTP.nullVecsMS ms Nothing

  K.logLE K.Info "PUMAs"
  (te, teP, tr, trP) <- buildCachedTestTrain @[GT.StateAbbreviation, GT.PUMA] (view GT.stateAbbreviation) (view GT.pUMA) ms "test/demographics/PUMAs_AS_AE/" allPUMAs_C
  reportVariations nullVecsSVD (te, teP) (tr, trP)


asAE_TractsFromPUMAs :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => K.Sem r ()
asAE_TractsFromPUMAs = do
  K.logLE K.Info "Tracts from PUMAs"
  let ms = DMC.marginalStructure @DMC.ASE  @'[DT.SexC] @'[DT.Education4C] @DMS.CellWithDensity @'[DT.Age5C] DMS.cwdWgtLens DMS.innerProductCWD
  tractTables_C <- logTime "Load Tables" $ ACS.loadACS_2017_2022_Tracts
  allTractsASE_C <- logTime "Aggregate..."
                    $ BRCC.retrieveOrMakeFrame "test/demographics/allTractsASE.bin" tractTables_C
                    $ pure . (aggregateAndZeroFillTables @TractGeoR @DMC.ASE . fmap F.rcast . ACS.ageSexEducation)
  tractMs <- cachedFullAndProduct @[GT.StateAbbreviation, GT.TractGeoId] ms "test/demographics/Tracts_AS_AE/" "Full" allTractsASE_C
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
  allPUMAsASE_C <- logTime "PUMA data to cache"
                   $ fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASE . fmap F.rcast)
                   <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
  pumaMs <- cachedFullAndProduct  @[GT.StateAbbreviation, GT.PUMA] ms "test/demographics/PUMAs_AS_AE/" "Full" allPUMAsASE_C
   -- use PUMAs to train, tracts to test
  nullVecsSVD <- K.knitMaybe "asAE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                 $  DTP.nullVecsMS ms Nothing
  reportVariations nullVecsSVD tractMs pumaMs


reportVariations :: (K.KnitMany r, K.KnitEffects r)
                 => DTP.NullVectorProjections k -> (LA.Matrix Double, LA.Matrix Double) -> (LA.Matrix Double, LA.Matrix Double) -> K.Sem r()
reportVariations nullVecs (testM, testProductsM) (trainingM, trainingProductsM) = do
  let nTest = LA.cols testM
      nCells = LA.rows testM
  K.logLE K.Info $ "N_testing = " <> show nTest <> "; N_training = " <> show (LA.cols trainingM)
  reportErrors "Product" testM testProductsM
{-  let testingAlpha = trueAlpha nullVecs testM testProductsM
      ta2M = LA.tr testingAlpha LA.<> testingAlpha
      ta2V = LA.takeDiag ta2M
      ta2 = VS.sum (VS.map sqrt ta2V) / realToFrac (VS.length ta2V)
  K.logLE K.Info $ "E[||alpha||_2] = " <> show ta2 <> "; E[||alpha||_2]/sqrt(N)=" <> show (ta2 / sqrt(realToFrac nCells))
-}
  let trainingAlpha = trueAlpha nullVecs trainingM trainingProductsM
      aaModel = averageAlphaModel trainingAlpha
  aa <- logTime "<alpha>" $ estimate nullVecs aaModel noOnSimplex testProductsM testProductsM
  reportErrors "<alpha>" testM aa
  aaOS <- logTime "OS(<alpha>)" $ estimate nullVecs aaModel (nearestOnSimplex nullVecs) testProductsM testProductsM
  reportErrors "OS(<alpha>)" testM aaOS
  aaSLSQP <- logTime "SLSQP(<alpha>)" $ estimate nullVecs aaModel (euclideanSLSQP nullVecs) testProductsM testProductsM
  reportErrors "SLSQP(<alpha>)" testM aaSLSQP
  aaNNLS <- logTime "NNLS(<alpha>)" $ estimate nullVecs aaModel (\x y -> DED.mapPE $ nnls DTP.semThrow nullVecs x y) testProductsM testProductsM
  reportErrors "NNLS(<alpha>)" testM aaNNLS
  let alphaLRModel = alphaProductLR trainingProductsM trainingAlpha
  lraSLSQP <- logTime "SLSQP(alpha_LR)" $ estimate nullVecs alphaLRModel (euclideanSLSQP nullVecs) testProductsM testProductsM
  reportErrors "SLSQP(alpha_LR)" testM lraSLSQP

--  wSLSQP <- logTime "wSLSQP(<alpha>; 1/sqrt(x))" $ estimate nullVecsSVD aaModel (weightedSLSQP nullVecsSVD (\x -> 1/sqrt x)) testProductsM testProductsM
--  reportErrors "wSLSQP(<alpha>; 1/sqrt(x))" testM wSLSQP

{-
  trainingTracts <- K.ignoreCacheTime training_C
  tractASEToCSV "Tracts_ASE_Training.csv" $ fmap F.rcast trainingTracts
  testTracts <- K.ignoreCacheTime testing_C
  tractASEToCSV "Tracts_ASE_Testing.csv" $ fmap F.rcast testTracts
-}
  pure ()


asAE_average_stuff :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => K.Sem r ()
asAE_average_stuff = do
  let (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
  testPUMAs_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASE . fmap F.rcast)
                 <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
  (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                     $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) testPUMAs_C
  testPUMAs <- K.ignoreCacheTime testPUMAs_C
  let ms = DMC.marginalStructure @DMC.ASE  @'[DT.SexC] @'[DT.Education4C] @DMS.CellWithDensity @'[DT.Age5C] DMS.cwdWgtLens DMS.innerProductCWD'
  nullVecsSVD <- K.knitMaybe "asAE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                 $  DTP.nullVecsMS ms Nothing

  let catMapASE :: DNS.CatMap (F.Record DMC.ASE) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                            , ('S', S.size $ Keyed.elements @DT.Sex)
                                                            , ('E', S.size $ Keyed.elements @DT.Education4)
                                                            ]
      catAndKnownAS_AE :: DNS.CatsAndKnowns (F.Record DMC.ASE) = DNS.CatsAndKnowns catMapASE
                                                                 [DNS.KnownMarginal "AS", DNS.KnownMarginal "AE"]
  nullVecsTensor <- K.knitMaybe "asAE_average_stuff: Problem constructing nullVecs. Bad marginal subsets?"
                    $  DTP.nullVecsMS ms (Just catAndKnownAS_AE)

  let covFld :: FL.Fold (F.Record (DMC.ASE V.++ '[DT.PopCount, DT.PWPopPerSqMile])) [Double]
      covFld = fmap VS.toList $ DMC.innerFoldWD (F.rcast @DMC.AS) (F.rcast @DMC.AE)
      popVecFld = DED.vecFld getSum (Sum . realToFrac . view DT.popCount) (F.rcast @DMC.ASE)
      popFld = FL.premap (realToFrac . view DT.popCount) FL.sum
      safeDiv x y = if y /= 0 then x / y else 0
      alphaFld nv = (\p -> VS.toList . DTP.fullToProj nv . VS.map (flip safeDiv p)) <$> popFld <*> popVecFld
      alphaCovInner nv k = (\as cs -> AlphaCov (k ^. GT.stateAbbreviation) (k ^. GT.pUMA) as cs) <$> alphaFld nv <*> covFld
      alphaCovFld :: DTP.NullVectorProjections (F.Record DMC.ASE) -> FL.Fold (F.Record (DMC.PUMARowR DMC.ASE)) [AlphaCov]
      alphaCovFld nv = MR.mapReduceFold
                       MR.noUnpack
                       (MR.assign (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @(DMC.ASE V.++ '[DT.PopCount, DT.PWPopPerSqMile])))
                       (MR.ReduceFold $ alphaCovInner nv)
      alphaCovs nv = FL.fold (alphaCovFld nv) testPUMAs

  let avgAlphaFld n = FL.premap (\ac -> acAlphas ac List.!! n) FL.mean
      avgAlphasFld = fmap VS.fromList $ traverse avgAlphaFld [0..119]
      avgAlphas nv = FL.fold avgAlphasFld $ alphaCovs nv
      avgChange nv = zip (S.toList $ Keyed.elements @(F.Record DMC.ASE)) $ VS.toList $ DTP.projToFull nv (avgAlphas nv)
      showKey k = T.intercalate ", " $ [show (k ^. DT.age5C), show (k ^. DT.sexC), show (k ^. DT.education4C)]
  K.logLE K.Info $ "Computing total deviation of products..."
  let pumasToMatFld :: FL.Fold (F.Record (DMC.PUMARowR DMC.ASE)) (LA.Matrix Double)
      pumasToMatFld = labeledToMatFld (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @DMC.ASE) (realToFrac . view DT.popCount)
      pumasToProdMatFld :: FL.Fold (F.Record (DMC.PUMARowR DMC.ASE)) (LA.Matrix Double)
      pumasToProdMatFld = labeledToProdMatFld DMS.cwdWgtLens ms (F.rcast @[GT.StateAbbreviation, GT.PUMA]) (F.rcast @DMC.ASE) DTM3.cwdF
  (pumasTM, pumaProductsTM) <- fmap (first matrix . second matrix)
                               $ K.ignoreCacheTimeM
                               $ BRCC.retrieveOrMakeD "test/AS_AE/pumaMatricesTesting.bin" pumaTesting_C $ \x -> do
    let (pumas, products) = FL.fold ((,) <$> pumasToMatFld <*> pumasToProdMatFld) x
    pure $ (FlatMatrix pumas, FlatMatrix products)
  (pumasTrM, pumaProductsTrM) <- fmap (first matrix . second matrix)
                               $ K.ignoreCacheTimeM
                               $ BRCC.retrieveOrMakeD "test/AS_AE/pumaMatricesTraining.bin" pumaTraining_C $ \x -> do
    let (pumas, products) = FL.fold ((,) <$> pumasToMatFld <*> pumasToProdMatFld) x
    pure $ (FlatMatrix pumas, FlatMatrix products)
  (pumasJTrM, pumaProductsJTrM) <- fmap (first matrix . second matrix)
                                   $ K.ignoreCacheTimeM
                                   $ BRCC.retrieveOrMakeD "test/AS_AE/pumaJMatricesTraining.bin" pumaTraining_C $ \x -> do
    let (pumas, products) = FL.fold ((,) <$> pumasToMatFld <*> pumasToProdMatFld) x
        jitterVec r =
          let
            v = addToAll 100 r
            s = VS.sum v / VS.sum r
          in VS.map (/ s) v
        jitterRows = LA.fromRows . fmap jitterVec . LA.toRows
    pure $ (FlatMatrix $ jitterRows pumas, FlatMatrix $ jitterRows products)

--  K.logLE K.Info $ toText $ LA.dispf 4 pumasM
--  K.logLE K.Info $ toText $ LA.dispf 4 pumaProductsM

--      pumaProductsM = FL.fold pumasToProdMatFld testPUMAs
  let totalDeviations x = zipWith totalDeviation (LA.toColumns pumasTM) (LA.toColumns x)
      avgDeviation x = FL.fold FL.mean $ totalDeviations x
      nPUMA = LA.cols pumasTM
  K.logLE K.Info $ "N_puma=" <> show nPUMA
  K.logLE K.Info $ "avg deviation=" <> show (avgDeviation pumaProductsTM)
  let diffs = pumasTrM - pumaProductsTrM
      alphasSVD = DTP.fullToProjM nullVecsSVD LA.<> diffs
      (meanAlphaSVD, covAlphaSVD) = LA.meanCov $ LA.tr alphasSVD
  K.logCat "AlphaMismatch" K.Diagnostic $ "SVD null-space basis (cols):" <> toText (LA.disps 4 (LA.tr $ DTP.fullToProjM nullVecsSVD))
  K.logLE K.Info $ "SVD alphas=" <> DED.prettyVector meanAlphaSVD
  K.logLE K.Info $ "SVD alpha covs=" <> (toText $ LA.disps 4 $ LA.unSym covAlphaSVD)
  let alphasTensor = DTP.fullToProjM nullVecsTensor LA.<> diffs
      (meanAlphaTensor, covAlphaTensor) = LA.meanCov $ LA.tr alphasTensor
  K.logCat "AlphaMismatch" K.Diagnostic $ "Tensor null-space basis (cols):" <> toText (LA.disps 4 (LA.tr $ DTP.fullToProjM nullVecsTensor))
  K.logLE K.Info $ "Tensor alphas=" <> DED.prettyVector meanAlphaTensor
  K.logLE K.Info $ "Tensor alpha covs=" <> (toText $ LA.disps 4 $ LA.unSym covAlphaTensor)
  let alphasJSVD = DTP.fullToProjM nullVecsSVD LA.<> (pumasJTrM - pumaProductsJTrM)
      (meanAlphaJSVD, covJAlphaSVD) = LA.meanCov $ LA.tr alphasJSVD
      prodAvgAlphaSVD = pumaProductsTM + LA.fromColumns (replicate nPUMA $ DTP.projToFullM nullVecsSVD LA.#> meanAlphaSVD)
      prodAvgAlphaTensor = pumaProductsTM + LA.fromColumns (replicate nPUMA $ DTP.projToFullM nullVecsTensor LA.#> meanAlphaTensor)
      prodAvgAlphaJSVD = pumaProductsTM + LA.fromColumns (replicate nPUMA $ DTP.projToFullM nullVecsSVD LA.#> meanAlphaJSVD)
--      aaTotalDeviations = zipWith totalDeviation (LA.toColumns pumasTM) (LA.toColumns prodAvgAlpha)
      avgAADeviationSVD  = FL.fold FL.mean $ totalDeviations prodAvgAlphaSVD --aaTotalDeviations
      avgAADeviationJSVD  = FL.fold FL.mean $ totalDeviations prodAvgAlphaJSVD --aaTotalDeviations
      avgAADeviationTensor  = FL.fold FL.mean $ totalDeviations prodAvgAlphaTensor --aaTotalDeviations
  K.logLE K.Info $ "SVD: avg AA deviation=" <> show avgAADeviationSVD
  K.logLE K.Info $ "JSVD: avg AA deviation=" <> show avgAADeviationJSVD
  K.logLE K.Info $ "Tensor: avg AA deviation=" <> show avgAADeviationTensor

  let aaOSSVD = LA.fromColumns $ fmap DTP.projectToSimplex $ LA.toColumns prodAvgAlphaSVD
      aaOSJSVD = LA.fromColumns $ fmap DTP.projectToSimplex $ LA.toColumns prodAvgAlphaJSVD
      avgAAOSDeviationSVD = FL.fold FL.mean $ totalDeviations aaOSSVD
      avgAAOSDeviationJSVD = FL.fold FL.mean $ totalDeviations aaOSJSVD
      aaOSTensor = LA.fromColumns $ fmap DTP.projectToSimplex $ LA.toColumns prodAvgAlphaTensor
      avgAAOSDeviationTensor = FL.fold FL.mean $ totalDeviations aaOSTensor
  K.logLE K.Info $ "SVD: avg AAOS deviation=" <> show avgAAOSDeviationSVD
  K.logLE K.Info $ "JSVD: avg AAOS deviation=" <> show avgAAOSDeviationJSVD
  K.logLE K.Info $ "Tensor: avg AAOS deviation=" <> show avgAAOSDeviationTensor
  K.logLE K.Info $ "doing full vector optimization"
  let optimize nv (p, t) = DED.mapPE $ DTP.optimalVector nv p t
      optimizerDataSVD = zip (LA.toColumns pumaProductsTM) (LA.toColumns aaOSSVD)
  aaOVSVD <- logTime "Full Vector (SVD)" (LA.fromColumns <$> traverse (optimize nullVecsSVD) optimizerDataSVD)
  let avgAAOVDeviationSVD = FL.fold FL.mean $ totalDeviations aaOVSVD
  K.logLE K.Info $ "SVD: avg AAOV deviation=" <> show avgAAOVDeviationSVD
  let optimizerDataTensor = zip (LA.toColumns pumaProductsTM) (LA.toColumns aaOSTensor)
  aaOVTensor <- logTime "Full Vector (SVD)" (LA.fromColumns <$> traverse (optimize nullVecsTensor) optimizerDataTensor)
  let avgAAOVDeviationTensor = FL.fold FL.mean $ totalDeviations aaOVTensor
  K.logLE K.Info $ "Tensor: avg AAOV deviation=" <> show avgAAOVDeviationTensor

  K.logLE K.Info $ "doing weight vector optimization"
  let owOptimize nV (pV, aV) = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull nV aV pV
      owOptimizerData x = zip (LA.toColumns pumaProductsTM) (replicate nPUMA x)
      toJoint nv alphas = pumaProductsTM + DTP.projToFullM nv LA.<> alphas
  aaOWTensor <- logTime "Weights (Tensor)" (LA.fromColumns <$> (traverse (owOptimize nullVecsTensor) $ owOptimizerData meanAlphaTensor))
  aaOWSVD <- logTime "Weights (SVD)" (LA.fromColumns <$> (traverse (owOptimize nullVecsSVD) $ owOptimizerData meanAlphaSVD))
  K.logLE K.Info $ "avg Tensor AAOW deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsTensor aaOWTensor)
  K.logLE K.Info $ "avg SVD AAOW deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaOWSVD)

  K.logLE K.Info $ "doing weight vector optimization, invWeight"
  let iwOptimize f nV (pV, aV) = DED.mapPE $ DTP.optimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) (DTP.euclideanWeighted f) nV aV pV
      tryF t f = do
        aaIWTensor <- logTime "Weighted (Tensor)" (LA.fromColumns <$> (traverse (iwOptimize f nullVecsTensor) $ owOptimizerData meanAlphaTensor))
        aaIWSVD <- logTime "Weighted (SVD)" (LA.fromColumns <$> (traverse (iwOptimize f nullVecsSVD) $ owOptimizerData meanAlphaSVD))
        K.logLE K.Info $ "avg Tensor AAIW deviation (f is " <> t <> ")=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsTensor aaIWTensor)
        K.logLE K.Info $ "avg SVD AAIW deviation (f is" <> t <> ")=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaIWSVD)
--  tryF "-log x" (negate . Numeric.log)
--  tryF "1/x" (\x -> 1/x)
--  tryF "x^(-0.75)" (\x -> x ** (-0.75))
  tryF "1/sqrt(x)" (\x -> 1/sqrt x)
--  tryF "x^(-0.25)" (\x -> x ** (-0.25))
  K.logLE K.Info $ "Weight vector optimization via NNLS"
  let nnlsConfig = DTP.defaultActiveSetConfig { DTP.cfgLogging = DTP.LogOnError, DTP.cfgStart = DTP.StartGS, DTP.cfgSolver = DTP.SolveLS }
      nnlsLSI_E nV = LH.precomputeFromE $ DTP.projToFullM nV
      leftError me = do
        e <- me
        case e of
          Left msg -> K.knitError msg
          Right x -> pure x
      asOptimize nV  mLSI_E (pV, aV) = DED.mapPE $ DTP.optimalWeightsAS DTP.semThrow (Just (\x -> 1 / sqrt x)) mLSI_E nV aV pV
--  aaASTensor <- LA.fromColumns <$> (traverse (asOptimize nullVecsTensor) $ owOptimizerData meanAlphaTensor)
  aaASSVD <- logTime "NNLS (SVD), naive" (LA.fromColumns <$> (traverse (asOptimize nullVecsSVD Nothing) $ owOptimizerData meanAlphaSVD))
  K.logLE K.Info $ "avg SVD AAAS deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaASSVD)
  aaASSVD' <- logTime "NNLS (SVD), precompute" (LA.fromColumns <$> (traverse (asOptimize nullVecsSVD (Just $ nnlsLSI_E nullVecsSVD)) $ owOptimizerData meanAlphaSVD))
--  K.logLE K.Info $ "avg Tensor AAAS deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsTensor aaASTensor)
  K.logLE K.Info $ "avg SVD AAAS deviation=" <> show (FL.fold FL.mean $ totalDeviations $ toJoint nullVecsSVD aaASSVD')
--      totalDev
  pure ()
--      allPUMAMat = FL.fold pumasToMatFld testPUMAs


compareAS_AE :: forall r . (K.KnitMany r, DED.EnrichDataEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareAS_AE cmdLine postInfo = do
    K.logLE K.Info "Building test PUMA-level products for AS x AE -> ASE"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASE . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C

    let optimalOnSimplex = DTP.viaNearestOnSimplex --DTP.viaOptimalWeights DTP.euclideanFull
    (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                       $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) byPUMA_C
    pumaSVD_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASE @'[DT.SexC] @'[DT.Education4C]
                  optimalOnSimplex
                  (Left "AS_AE_svd")
                  (Left "model/demographic/AS_AE_svd")
                  (const DTM3.Mean) --(DTM3.Model . tsModelConfig "AS_AE_svd")
                  (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C

    (pumaProductSVD, pumaSVD) <- K.ignoreCacheTime pumaSVD_C

    let catMap :: DNS.CatMap (F.Record DMC.ASE) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                            , ('S', S.size $ Keyed.elements @DT.Sex)
                                                            , ('E', S.size $ Keyed.elements @DT.Education4)
                                                            ]
        catAndKnownsAll :: DNS.CatsAndKnowns (F.Record DMC.ASE) = DNS.CatsAndKnowns catMap
                                                                  [DNS.KnownMarginal "AS", DNS.KnownMarginal "AE"]
    pumaTensor_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASE @'[DT.SexC] @'[DT.Education4C]
                     optimalOnSimplex
                     (Left  "AS_AE_tensor")
                     (Left  "model/demographic/AS_AE_tensor")
                     (const DTM3.Mean) --(DTM3.Model . tsModelConfig "AS_AE_tensor")
                     (show . view GT.pUMA) cmdLine Nothing (Just ("Tensor", catAndKnownsAll)) Nothing pumaTraining_C pumaTesting_C
    (pumaProductTensor, pumaTensor) <- K.ignoreCacheTime pumaTensor_C

    pumaTesting <- K.ignoreCacheTime pumaTesting_C
    let showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C)
        pumaKey r = (r ^. GT.stateAbbreviation, r ^. GT.pUMA)
        testPUMASet = FL.fold (FL.premap pumaKey FL.set) pumaTesting
        filterToTestPUMAs = F.filterFrame ((`S.member` testPUMASet) . pumaKey)
        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
    pumaModelPaths <- postPaths "Model_AS_AE_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_AS_AS_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASE)
        pumaModelPaths postInfo False False "AS_AE" ("PUMA", pumaPopMap, pumaKey) Nothing --(Just "NY")
        (F.rcast @DMC.ASE)
        (\r -> (r ^. DT.sexC, r ^. DT.education4C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        Nothing --(Just ("Race", show . view DT.race5C, Just raceOrder))
        Nothing --(Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASE pumaTesting)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASE pumaTesting) (Just "Actual") Nothing Nothing
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductSVD) (Just "Product (SVD)") (Just "Product (SVD)") (Just "Product (SVD)")
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductTensor) (Just "Product (Tensor)") (Just "Product (Tensor)") (Just "Product (Tensor)")
        ,MethodResult (fmap F.rcast pumaSVD) (Just "SVD") (Just "SVD") (Just "SVD")
        ,MethodResult (fmap F.rcast pumaTensor) (Just "Tensor") (Just "Tensor") (Just "Tensor")
        ]


compareAS_AR :: forall r . (K.KnitMany r, DED.EnrichDataEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareAS_AR cmdLine postInfo = do
    K.logLE K.Info "Building test PUMA-level products for AS x AR -> ASR"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASR . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C

    let optimalOnSimplex = DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull
    (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                       $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) byPUMA_C
    pumaSVD_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASR @'[DT.SexC] @'[DT.Race5C]
                  optimalOnSimplex
                  (Left "AS_AR_svd")
                  (Left "model/demographic/AS_AR_svd")
                  (DTM3.Model . tsModelConfig "AS_AR_svd")
                  (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C

    (pumaProductSVD, pumaSVD) <- K.ignoreCacheTime pumaSVD_C

    let catMap :: DNS.CatMap (F.Record DMC.ASR) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                            , ('S', S.size $ Keyed.elements @DT.Sex)
                                                            , ('R', S.size $ Keyed.elements @DT.Race5)
                                                            ]
        catAndKnownsAll :: DNS.CatsAndKnowns (F.Record DMC.ASR) = DNS.CatsAndKnowns catMap
                                                                  [DNS.KnownMarginal "AS", DNS.KnownMarginal "AR"]
    pumaTensor_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASR @'[DT.SexC] @'[DT.Race5C]
                     (DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull)
                     (Left  "AS_AR_tensor")
                     (Left  "model/demographic/AS_AR_tensor")
                     (const DTM3.Mean) --(DTM3.Model . tsModelConfig "AS_AE_tensor")
                     (show . view GT.pUMA) cmdLine Nothing (Just ("Tensor", catAndKnownsAll)) Nothing pumaTraining_C pumaTesting_C
    (pumaProductTensor, pumaTensor) <- K.ignoreCacheTime pumaTensor_C

    pumaTesting <- K.ignoreCacheTime pumaTesting_C
    let showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.race5C)
        pumaKey r = (r ^. GT.stateAbbreviation, r ^. GT.pUMA)
        testPUMASet = FL.fold (FL.premap pumaKey FL.set) pumaTesting
        filterToTestPUMAs = F.filterFrame ((`S.member` testPUMASet) . pumaKey)
        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
    pumaModelPaths <- postPaths "Model_AS_AR_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_AS_AR_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASR)
        pumaModelPaths postInfo False False "AS_AR" ("PUMA", pumaPopMap, pumaKey) Nothing --(Just "NY")
        (F.rcast @DMC.ASR)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.race5C, r ^. DT.age5C))
        showCellKey
        Nothing --(Just ("Race", show . view DT.race5C, Just raceOrder))
        Nothing --(Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASR pumaTesting)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASR pumaTesting) (Just "Actual") Nothing Nothing
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductSVD) (Just "Product (SVD)") (Just "Product (SVD)") (Just "Product (SVD)")
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductTensor) (Just "Product (Tensor)") (Just "Product (Tensor)") (Just "Product (Tensor)")
        ,MethodResult (fmap F.rcast pumaSVD) (Just "SVD") (Just "SVD") (Just "SVD")
        ,MethodResult (fmap F.rcast pumaTensor) (Just "Tensor") (Just "Tensor") (Just "Tensor")
        ]


compareASR_ASE' :: forall r . (K.KnitMany r, DED.EnrichDataEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareASR_ASE' cmdLine postInfo = do
    K.logLE K.Info "Building test PUMA-level products for ASR x ASE -> ASER"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C

    let optimalOnSimplex = DTP.viaNearestOnSimplex --DTP.viaOptimalWeights DTP.euclideanFull
    (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                       $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) byPUMA_C
    pumaSVD_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                  optimalOnSimplex
                  (Left "ASR_ASE_svd")
                  (Left "model/demographic/ASR_ASE_svd")
                  (const DTM3.Mean) -- (DTM3.Model . tsModelConfig "ASR_ASE_svd")
                  (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C

    (pumaProductSVD, pumaSVD) <- K.ignoreCacheTime pumaSVD_C

    let catMap :: DNS.CatMap (F.Record DMC.ASER) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                                , ('S', S.size $ Keyed.elements @DT.Sex)
                                                                , ('E', S.size $ Keyed.elements @DT.Education4)
                                                                , ('R', S.size $ Keyed.elements @DT.Race5)
                                                                ]
        catAndKnownsAll :: DNS.CatsAndKnowns (F.Record DMC.ASER) = DNS.CatsAndKnowns catMap
                                                                  [DNS.KnownMarginal "ASE", DNS.KnownMarginal "ASR"]
    pumaTensor_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                     DTP.viaNearestOnSimplex
                     (Left  "ASR_ASE_tensor")
                     (Left  "model/demographic/ASR_ASE_tensor")
                     (const DTM3.Mean) --(DTM3.Model . tsModelConfig "ASR_ASE_tensor")
                     (show . view GT.pUMA) cmdLine Nothing (Just ("Tensor", catAndKnownsAll)) Nothing pumaTraining_C pumaTesting_C
    (pumaProductTensor, pumaTensor) <- K.ignoreCacheTime pumaTensor_C

    pumaTesting <- K.ignoreCacheTime pumaTesting_C
    let showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
        pumaKey r = (r ^. GT.stateAbbreviation, r ^. GT.pUMA)
        testPUMASet = FL.fold (FL.premap pumaKey FL.set) pumaTesting
        filterToTestPUMAs = F.filterFrame ((`S.member` testPUMASet) . pumaKey)
        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
    pumaModelPaths <- postPaths "Model_ASR_ASE_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_ASR_ASE_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASER)
        pumaModelPaths postInfo False False "ASR_ASE" ("PUMA", pumaPopMap, pumaKey) Nothing --(Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        Nothing --(Just ("Race", show . view DT.race5C, Just raceOrder))
        Nothing --(Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaTesting)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaTesting) (Just "Actual") Nothing Nothing
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductSVD) (Just "Product (SVD)") (Just "Product (SVD)") (Just "Product (SVD)")
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaProductTensor) (Just "Product (Tensor)") (Just "Product (Tensor)") (Just "Product (Tensor)")
        ,MethodResult (fmap F.rcast pumaSVD) (Just "SVD") (Just "SVD") (Just "SVD")
        ,MethodResult (fmap F.rcast pumaTensor) (Just "Tensor") (Just "Tensor") (Just "Tensor")
        ]

compareASR_ASE :: forall r . (K.KnitMany r, DED.EnrichDataEffects r, BRCC.CacheEffects r, K.Member PR.RandomFu r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareASR_ASE cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for ASR x ASE -> ASER"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) = ACS.acs1Yr2012_22 @r
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = byPUMA_C
    testPUMAs <- K.ignoreCacheTime testPUMAs_C

--    testCDs_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.ASER . fmap F.rcast)
--                 <$> DDP.cachedACSa5ByCD srcWindow cachedSrc 2020 Nothing
--    testCDs <- K.ignoreCacheTime testCDs_C

    let optimalOnSimplex = DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull
--    let optimalOnSimplex = DTP.viaNearestOnSimplex
--    let optimalOnSimplex = DTP.viaNearestOnSimplexPlus

    pumaTestRaw_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                      optimalOnSimplex
                      (Right "ASR_ASE_ByPUMA")
                      (Right  "model/demographic/asr_ase")
                      (DTM3.Model . tsModelConfig "ASR_ASE_ByPUMA")
                      (show . view GT.pUMA) cmdLine Nothing Nothing Nothing byPUMA_C testPUMAs_C
    (pumaProduct, pumaModeledRaw) <- K.ignoreCacheTime pumaTestRaw_C

    (pumaTraining_C, pumaTesting_C) <- KC.wctSplit
                                       $ KC.wctBind (splitGEOs testHalf (view GT.stateAbbreviation) (view GT.pUMA)) byPUMA_C
    pumaTestSplit_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                        optimalOnSimplex
                        (Right "ASR_ASE_TestHalfPUMA")
                        (Right "model/demographic/testHalf_asr_ase")
                        (DTM3.Model . tsModelConfig "ASR_ASE_TestHalfPUMA")
                        (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C

    (pumaSplitProduct, pumaSplitFull) <- K.ignoreCacheTime pumaTestSplit_C
{-
    pumaTestSplit10_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                          optimalOnSimplex
                          (Right "ASR_ASE_TestHalfPUMA_10")
                          (Right "model/demographic/testHalf_asr_ase_10")
                          (DTM3.Model . tsModelConfig "ASR_ASE_TestHalfPUMA")
                          (show . view GT.pUMA) cmdLine Nothing Nothing (Just 0.1) pumaTraining_C pumaTesting_C
    (_, pumaSplitFull10) <- K.ignoreCacheTime pumaTestSplit10_C

    pumaTestSplit50_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                          optimalOnSimplex
                          (Right "ASR_ASE_TestHalfPUMA_50")
                          (Right "model/demographic/testHalf_asr_ase_50")
                          (DTM3.Model . tsModelConfig "ASR_ASE_TestHalfPUMA")
                          (show . view GT.pUMA) cmdLine Nothing Nothing (Just 0.5) pumaTraining_C pumaTesting_C
    (_, pumaSplitFull50) <- K.ignoreCacheTime pumaTestSplit50_C


    pumaTestSplit90_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                          optimalOnSimplex
                          (Right "ASR_ASE_TestHalfPUMA_90")
                          (Right "model/demographic/testHalf_asr_ase_90")
                          (DTM3.Model . tsModelConfig "ASR_ASE_TestHalfPUMA")
                          (show . view GT.pUMA) cmdLine Nothing Nothing (Just 0.9) pumaTraining_C pumaTesting_C
    (_, pumaSplitFull90) <- K.ignoreCacheTime pumaTestSplit90_C

    pumaTestSplitTH_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                          optimalOnSimplex
                          (Right "ASR_ASE_TestHalfPUMA_TH")
                          (Right "model/demographic/testHalf_TH_asr_ase")
                          (DTM3.Model . thModelConfig "ASR_ASE_TestHalfPUMA_TH")
                          (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C
    (_, pumaSplitFullTH) <- K.ignoreCacheTime pumaTestSplitTH_C

    pumaSplitMean_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                       optimalOnSimplex
                       (Right "ASR_ASE_ByPUMA")
                       (Right "model/demographic/mean_asr_ase")
                       (const DTM3.Mean)
                       (show . view GT.pUMA) cmdLine Nothing Nothing Nothing pumaTraining_C pumaTesting_C
    (_, pumaSplitMean) <- K.ignoreCacheTime pumaSplitMean_C
-}
{-
    let aRE = DED.averagingMatrix @(F.Record DMC.ASRE) @(F.Record [DT.Race5C, DT.Education4C]) F.rcast
        mId :: LA.Matrix Double = LA.ident 200
    pumaTestAvgER_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                        (DTP.viaOptimalWeights )
                        (Right  "ASR_ASE_ByPUMA")
                        (Right  "model/demographic/asr_ase_AvgRE")
                        False
                        (show . view GT.pUMA) cmdLine (Just aRE) Nothing byPUMA_C testPUMAs_C

    (_, pumaModeledAvgER) <- K.ignoreCacheTime pumaTestAvgER_C
-}
{-
    let aSRE = DED.averagingMatrix @(F.Record DMC.ASRE) @(F.Record [DT.SexC, DT.Race5C, DT.Education4C]) F.rcast
        mPlus mSoFar mNew = mSoFar + mNew LA.<> (mId - mSoFar)
        mRE_SRE = mPlus aRE aSRE --aRE + aSRE LA.<> (mId - aRE)
    pumaTestAvgER_SER_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                            (DTP.viaOptimalWeights  DTP.euclideanFull)
                            (Right  "ASR_ASE_ByPUMA")
                            (Right  "model/demographic/asr_ase_AvgER_SER")
                            False
                            (show . view GT.pUMA) cmdLine (Just mRE_SRE) Nothing byPUMA_C testPUMAs_C
    (_, pumaModeledAvgER_SER) <- K.ignoreCacheTime pumaTestAvgER_SER_C
    let aARE = DED.averagingMatrix @(F.Record DMC.ASRE) @(F.Record [DT.Age5C, DT.Race5C, DT.Education4C]) F.rcast
        mRE_ARE = mPlus aRE aARE --aRE + aARE LA.<> (mId - aRE)
    pumaTestAvgER_AER_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                            (DTP.viaOptimalWeights DTP.euclideanFull)
                            (Right  "ASR_ASE_ByPUMA")
                            (Right  "model/demographic/asr_ase_AvgER_AER")
                            False
                            (show . view GT.pUMA) cmdLine (Just mRE_ARE) Nothing byPUMA_C testPUMAs_C
    (_, pumaModeledAvgER_AER) <- K.ignoreCacheTime pumaTestAvgER_AER_C

    let aASRE = DED.averagingMatrix @(F.Record DMC.ASRE) @(F.Record [DT.Age5C, DT.SexC, DT.Race5C, DT.Education4C]) F.rcast

        mRE_SRE_ARE = mPlus mRE_SRE aRE --aRE + aARE LA.<> (mId - aRE)
        mRE_SRE_ARE_ASRE = mPlus mRE_SRE_ARE aASRE
    pumaTestAvgER_SER_AER_ASER_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                                     (DTP.viaOptimalWeights DTP.euclideanFull)
                                     (Right  "ASR_ASE_ByPUMA")
                                     (Right  "model/demographic/asr_ase_AvgER_SER_AER_ASER")
                                     False
                                     (show . view GT.pUMA) cmdLine (Just mRE_SRE_ARE_ASRE) Nothing byPUMA_C testPUMAs_C
    (_, pumaModeledAvgER_SER_AER_ASER) <- K.ignoreCacheTime pumaTestAvgER_SER_AER_ASER_C
-}

    let catMap :: DNS.CatMap (F.Record DMC.ASER) = DNS.mkCatMap [('A', S.size $ Keyed.elements @DT.Age5)
                                                                , ('S', S.size $ Keyed.elements @DT.Sex)
                                                                , ('E', S.size $ Keyed.elements @DT.Education4)
                                                                , ('R', S.size $ Keyed.elements @DT.Race5)
                                                                ]
        catAndKnownsAll :: DNS.CatsAndKnowns (F.Record DMC.ASER) = DNS.CatsAndKnowns catMap
                                                                   [DNS.KnownMarginal "ASE", DNS.KnownMarginal "ASR"]
        catAndKnownsOnlyER :: DNS.CatsAndKnowns (F.Record DMC.ASER) = DNS.CatsAndKnowns catMap
                                                                      [DNS.KnownMarginal "ASE", DNS.KnownMarginal "ASR", DNS.KnownSubset "ASER"]
    pumaTestNewBasis_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
  --                         (DTP.viaOptimalWeights DTP.euclideanFull)
                           DTP.viaNearestOnSimplex
                           (Left  "ASR_ASE_NewBasis_ByPUMA")
                           (Left  "model/demographic/asr_ase_NewBasis")
                           (DTM3.Model . tsModelConfig "ASR_ASE_NewBasis_TestHalfPUMA")
                           (show . view GT.pUMA) cmdLine Nothing (Just ("NewBasis", catAndKnownsAll)) Nothing pumaTraining_C pumaTesting_C
    (_, pumaSplitNewBasis) <- K.ignoreCacheTime pumaTestNewBasis_C

    pumaTestOnlyER_C <-  testNS @DMC.PUMAOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
--                         (DTP.viaOptimalWeights DTP.euclideanFull)
                         DTP.viaNearestOnSimplex
                         (Left  "ASR_ASE_NewBasis_PlusER_ByPUMA")
                         (Left  "model/demographic/asr_ase_NewBasis_PlusER")
                         (DTM3.Model . tsModelConfig "ASR_ASE_NewBasis_PlusER_TestHalfPUMA")
                         (show . view GT.pUMA) cmdLine Nothing (Just ("NewBasis_OnlyER", catAndKnownsOnlyER)) Nothing pumaTraining_C pumaTesting_C
    (_, pumaSplitOnlyER) <- K.ignoreCacheTime pumaTestOnlyER_C

    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        pumaKey ok = (ok ^. GT.stateAbbreviation, ok ^. GT.pUMA)

        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testPUMAs
--        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) testCDs
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
    pumaTesting <- K.ignoreCacheTime pumaTesting_C
    let pumaKey r = (r ^. GT.stateAbbreviation, r ^. GT.pUMA)
        testPUMASet = FL.fold (FL.premap pumaKey FL.set) pumaTesting
        filterToTestPUMAs = F.filterFrame ((`S.member` testPUMASet) . pumaKey)
--    K.logLE K.Info $ "testPUMAs=" <> show testPUMASet
    pumaModelPaths <- postPaths "Model_ASR_ASE_ByPUMA" cmdLine
    BRK.brNewPost pumaModelPaths postInfo "Model_ASR_ASE_ByPUMA" $ do
{-
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASER)
        pumaModelPaths postInfo False False "ASR_ASE" ("PUMA", pumaPopMap, pumaKey) Nothing --(Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        Nothing --(Just ("Race", show . view DT.race5C, Just raceOrder))
        Nothing --(Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER testPUMAs)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER testPUMAs) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast pumaProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
--        ,MethodResult (fmap F.rcast pumaModeledAvgER) (Just "+E+R (via Avg)") (Just "+E+R (via Avg)") (Just "+E+R (via Avg)")
        ,MethodResult (fmap F.rcast pumaModeledOnlyER) (Just "+E+R") (Just "+E+R") (Just "+E+R")
--        ,MethodResult (fmap F.rcast pumaModeledAvgER_SER) (Just "NS: +E+R & +SER") (Just "NS: +E+R & +SER (L2)") (Just "NS: +E+R & +SER")
--        ,MethodResult (fmap F.rcast pumaModeledAvgER_AER) (Just "NS: +E+R & A+ER") (Just "NS: +E+R & A+ER (L2)") (Just "NS: +E+R & A+ER")
--        ,MethodResult (fmap F.rcast pumaModeledAvgER_SER_AER_ASER) (Just "NS: Full via avg") (Just "NS: Full via avg (L2)") (Just "NS: Full via avg")
        ,MethodResult (fmap F.rcast pumaModeledRaw) (Just "NS: Full") (Just "NS: Full") (Just "NS: Full")
--        ,MethodResult (fmap F.rcast pumaMeanFull) (Just "NS: Mean Only") (Just "NS: Mean Only (L2)") (Just "NS: Mean Only")
        ]
-}
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.ASER)
        pumaModelPaths postInfo False False "ASR_ASE" ("PUMA", pumaPopMap, pumaKey) Nothing --(Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        Nothing --(Just ("Race", show . view DT.race5C, Just raceOrder))
        Nothing --(Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaTesting)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.ASER pumaTesting) (Just "Actual") Nothing Nothing
        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaSplitProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
--        ,MethodResult (filterToTestPUMAs $ fmap F.rcast pumaRawFull) (Just "NS: Full") (Just "NS: Full") (Just "NS: Full")
        ,MethodResult (fmap F.rcast pumaSplitFull) (Just "NS: Split") (Just "NS: Split") (Just "NS: Split")
        ,MethodResult (fmap F.rcast pumaSplitNewBasis) (Just "NB: Split") (Just "NB: Split") (Just "NB: Split")
--        ,MethodResult (fmap F.rcast pumaSplitMean) (Just "NS: Split (Means)") (Just "NS: Split (Means)") (Just "NS: Split (Means)")
--        ,MethodResult (fmap F.rcast pumaSplitFull10) (Just "NS: Split (10%)") (Just "NS: Split (10%)") (Just "NS: Split (10%)")
--        ,MethodResult (fmap F.rcast pumaSplitFull50) (Just "NS: Split (50%)") (Just "NS: Split (50%)") (Just "NS: Split (50%)")
--        ,MethodResult (fmap F.rcast pumaSplitFull90) (Just "NS: Split (90%)") (Just "NS: Split (90%)") (Just "NS: Split (90%)")
--        ,MethodResult (fmap F.rcast pumaSplitFullTH) (Just "NS: Split (TH)") (Just "NS: Split (TH)") (Just "NS: Split (TH)")
        ,MethodResult (fmap F.rcast pumaSplitOnlyER) (Just "NB: Split (Only ER)") (Just "NB: Split (Only ER)") (Just "NB: Split (Only ER)")
        ]
    pumaASERToCSV "True_SplitTest.csv" $ fmap F.rcast pumaTesting
    pumaASERToCSV "Product_SplitTest.csv" $  F.filterFrame ((`S.member` testPUMASet) . pumaKey) $ fmap F.rcast pumaSplitProduct
    pumaASERToCSV "NS_SplitTest.csv" $ fmap F.rcast pumaSplitFull
    testPUMAs <- K.ignoreCacheTime testPUMAs_C
    pumaASERToCSV "True_Full.csv" $ fmap F.rcast testPUMAs
    pumaASERToCSV "Product_Full.csv" $  fmap F.rcast pumaProduct
    pumaASERToCSV "NS_Full.csv" $ fmap F.rcast pumaModeledRaw

{-
    cdFromPUMAModeledRaw_C <- DDP.cachedPUMAsToCDs @DMC.ASER "model/demographic/asr_ase_CDFromPUMA.bin" $ fmap snd pumaTestRaw_C
    cdFromPUMAModeledRaw <- K.ignoreCacheTime cdFromPUMAModeledRaw_C

    (cdProduct, cdModeledRaw) <- K.ignoreCacheTimeM
                                 $ testNS @DMC.CDOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                                 (DTP.viaOptimalWeights DTP.euclideanFull)
                                 (Right "ASR_ASE_ByPUMA")
                                 (Right "model/demographic/asr_ase")
                                 DDP.districtKeyT cmdLine Nothing Nothing Nothing byPUMA_C testCDs_C
    cdFromPUMAModeledAvgSE_C <- DDP.cachedPUMAsToCDs @DMC.ASER "model/demographic/asr_ase_AvgSE_CDFromPUMA.bin" $ fmap snd pumaTestAvgSE_C
    cdFromPUMAModeledAvgSE <- K.ignoreCacheTime cdFromPUMAModeledAvgSE_C

    (_, cdModeledAvgSE) <- K.ignoreCacheTimeM
                           $ testNS @DMC.CDOuterKeyR @DMC.ASER @'[DT.Race5C] @'[DT.Education4C]
                           (DTP.viaOptimalWeights DTP.euclideanFull)
                           (Right "ASR_ASE_ByPUMA")
                           (Right "model/demographic/asr_ase_AvgSE")
                           DDP.districtKeyT cmdLine (Just avgSE) Nothing Nothing byPUMA_C testCDs_C

    cdModelPaths <- postPaths "Model_ASR_ASE_ByCD" cmdLine
    BRCC.brNewPost cdModelPaths postInfo "Model_ASR_ASE_ByCD" $ do
      compareResults @(DMC.CDOuterKeyR V.++ DMC.ASER)
        cdModelPaths postInfo True "ASR_ASE" ("CD", cdPopMap, DDP.districtKeyT) (Nothing)
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.CDOuterKeyR @DMC.ASER testCDs)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.CDOuterKeyR @DMC.ASER testCDs) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast cdProduct) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast cdModeledAvgSE) (Just "Direct NS Model: 2nd order") (Just "Direct NS Model: 2nd order (L2)") (Just "Direct NS Model: 2nd order")
        ,MethodResult (fmap F.rcast cdModeledRaw) (Just "Direct NS Model") (Just "Direct NS Model (L2)") (Just "Direct NS Model")
        ,MethodResult (fmap F.rcast cdFromPUMAModeledAvgSE) (Just "Aggregated NS Model: 2nd order") (Just "Aggregated NS Model: 2nd order (L2)") (Just "Aggregated NS Model: 2nd order")
        ,MethodResult (fmap F.rcast cdFromPUMAModeledRaw) (Just "Aggregated NS Model") (Just "Aggregated NS Model (L2)") (Just "Aggregated NS Model")
        ]
-}
    pure ()

compareCASR_ASE :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareCASR_ASE cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for CASR x ASE -> CASER"
    let filterToState sa r = r ^. GT.stateAbbreviation == sa
        (srcWindow, cachedSrc) =  ACS.acs1Yr2012_22
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020
    let testPUMAs_C = {- fmap (F.filterFrame $ filterToState "RI") -} byPUMA_C
    byCD_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.CASER . fmap F.rcast)
              <$> DDP.cachedACSa5ByCD srcWindow cachedSrc 2020 Nothing
    byCD <- K.ignoreCacheTime byCD_C
    byPUMA <- K.ignoreCacheTime byPUMA_C
    (product_CASR_ASE, modeled_CASR_ASE) <- K.ignoreCacheTimeM
                                          $ testNS @DMC.PUMAOuterKeyR @DMC.CASER @'[DT.CitizenC, DT.Race5C] @'[DT.Education4C]
                                          (DTP.viaOptimalWeights DTP.defaultOptimalWeightsAlgoConfig (DTP.OWKLogLevel $ K.Debug 3) DTP.euclideanFull)
                                          (Right "CASR_ASE_ByPUMA")
                                          (Right "model/demographic/casr_ase")
                                          (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                                          (show . view GT.pUMA) cmdLine Nothing Nothing Nothing byPUMA_C testPUMAs_C

{-
    (product_CSR_ASR', modeled_CSR_ASR') <- K.ignoreCacheTimeM
                                            $ testProductNS_CD' @DMC.CASR @'[DT.CitizenC] @'[DT.Age5C]
                                            DTP.viaNearestOnSimplex False True "model/demographic/csr_asr" "CSR_ASR" cmdLine byPUMA_C byCD_C
-}
    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        pumaKey ok = (ok ^. GT.stateAbbreviation, ok ^. GT.pUMA)

        pumaPopMap = FL.fold (FL.premap (\r -> (pumaKey r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byPUMA
--        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
--        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.citizenC, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
    synthModelPaths <- postPaths "Model_CASR_ASE_ByPUMA" cmdLine
    BRK.brNewPost synthModelPaths postInfo "Model_CASR_ASE_ByPUMA" $ do
      compareResults @(DMC.PUMAOuterKeyR V.++ DMC.CASER)
        synthModelPaths postInfo False False "CASR_ASER" ("PUMA", pumaPopMap, pumaKey) (Just "NY")
        (F.rcast @DMC.CASER)
        (\r -> (r ^. DT.age5C, r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.citizenC, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.CASER byPUMA)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @DMC.PUMAOuterKeyR @DMC.CASER byPUMA) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_CASR_ASE) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_CASR_ASE) (Just "NS Model") (Just "NS Model (L2)") (Just "NS Model")
--        ,MethodResult (fmap F.rcast modeled_CSR_ASR') (Just "NS Model (Yi/Chen)") (Just "NS Model (Yi/Chen)") (Just "NS Model (Yi/Chen)")
        ]

{-
compareSER_ASR :: (K.KnitMany r, K.KnitEffects r, BRCC.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
compareSER_ASR cmdLine postInfo = do
    K.logLE K.Info "Building test CD-level products for SER x A6SR -> A6SER"
    byPUMA_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.ASER . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA ACS.acs1Yr2012_22 2020
    byCD_C <- fmap (aggregateAndZeroFillTables @DDP.ACSByCDGeoR @DMC.ASER . fmap F.rcast)
              <$> DDP.cachedACSa5ByCD ACS.acs1Yr2012_22 2020 Nothing
    byCD <- K.ignoreCacheTime byCD_C
    byPUMA <- K.ignoreCacheTime byPUMA_C
    (product_SER_ASR, modeled_SER_ASR) <- K.ignoreCacheTimeM
                                            $ testNS @CDLoc @DMC.ASER @'[DT.Age5C] @'[DT.Education4C]
                                            (DTP.viaOptimalWeights DTP.euclideanFull)
                                            (Right "SER_ASR")
                                            (Right "model/demographic/ser_asr")
                                            (DTM3.Model . tsModelConfig "CSR_ASR_ByPUMA")
                                            DDP.districtKeyT cmdLine Nothing Nothing byPUMA_C byCD_C
    let raceOrder = show <$> S.toList (Keyed.elements @DT.Race5)
        ageOrder = show <$> S.toList (Keyed.elements @DT.Age5)
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) byCD
        showCellKey r =  show (r ^. GT.stateAbbreviation, r ^. DT.age5C, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
    synthModelPaths <- postPaths "Model_SER_A6SR" cmdLine
    BRK.brNewPost synthModelPaths postInfo "Model_SER_A6SR" $ do
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.ASER)
        synthModelPaths postInfo True False "SER_ASR" ("CD", cdPopMap,  DDP.districtKeyT) (Just "NY")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @CDLoc @DMC.ASER byCD)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @CDLoc @DMC.ASER byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_SER_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_SER_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
      compareResults @([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict] V.++ DMC.ASER)
        synthModelPaths postInfo True False "SER_ASR" ("State", statePopMap, view GT.stateAbbreviation) (Just "PA")
        (F.rcast @DMC.ASER)
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.education4C, r ^. DT.age5C))
        showCellKey
        (Just ("Race", show . view DT.race5C, Just raceOrder))
        (Just ("Age", show . view DT.age5C, Just ageOrder))
        (ErrorFunction "L1 Error" l1Err pctFmt (* 100))
        (ErrorFunction "Standardized Mean" stdMeanErr pctFmt id)
        (fmap F.rcast $ testRowsWithZeros @CDLoc @DMC.ASER byCD)
        [MethodResult (fmap F.rcast $ testRowsWithZeros @CDLoc @DMC.ASER byCD) (Just "Actual") Nothing Nothing
        ,MethodResult (fmap F.rcast product_SER_ASR) (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult (fmap F.rcast modeled_SER_ASR) (Just "NS Model") (Just "NS Model") (Just "NS Model")
        ]
    pure ()
-}
main :: IO ()
main = do
  cmdLine ← CmdArgs.cmdArgsRun BR.commandLine
  pandocWriterConfig ←
    K.mkPandocWriterConfig
    pandocTemplate
    templateVars
    (BRK.brWriterOptionsF . K.mindocOptionsF)
  cacheDir <- toText . fromMaybe ".kh-cache" <$> Env.lookupEnv("BR_CACHE_DIR")
  let knitConfig ∷ K.KnitConfig BRCC.SerializerC BRCC.CacheData Text =
        (K.defaultKnitConfig $ Just cacheDir)
          { K.outerLogPrefix = Just "2022-Demographics"
          , K.logIf = BR.knitLogSeverity $ BR.logLevel cmdLine -- K.logDiagnostic
--          , K.lcSeverity = M.fromList [("KH_Cache", K.Special)]
          , K.pandocWriterConfig = pandocWriterConfig
          , K.serializeDict = BRCC.flatSerializeDict
          , K.persistCache = KC.persistStrictByteString (\t → toString (cacheDir <> "/" <> t))
          }
      randomGen = SR.mkStdGen 125 -- so we get the same results each time
  resE ← K.knitHtmls knitConfig $ PR.runStatefulRandom randomGen $ do
    K.logLE K.Info $ "Using cache-dir=" <> cacheDir
    K.logLE K.Info $ "Command Line: " <> show cmdLine
    let postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
{-    byPUMA_C <-  fmap (aggregateAndZeroFillTables @DDP.ACSByPUMAGeoR @DMC.CASR . fmap F.rcast)
                <$> DDP.cachedACSa5ByPUMA
    (predictor_C, _, _) <- DMC.predictorModel3 @'[DT.CitizenC] @'[DT.Age5C] @DMC.CASR @DMC.SR (Right "model/demographic/csr_asr") "CSR_ASR" cmdLine byPUMA_C

    sld2022CensusTables_C <- BRC.censusTablesFor2022SLDs
--    sldCensusTablesMA <- BRC.filterCensusTables ((== 25) . view GT.stateFIPS) <$> K.ignoreCacheTime sld2022CensusTables_C
--    BRK.logFrame $ BRC.ageSexEducation sldCensusTablesMA
    let censusTableCond r = r ^. GT.stateFIPS == 9 && r ^. GT.districtTypeC == GT.StateUpper && r ^. GT.districtName == "10"
        filteredCensusTables_C = fmap (BRC.filterCensusTables censusTableCond) sld2022CensusTables_C
    predictedSLDDemographics_C <- fmap fst $ DMC.predictedCensusCASR DTP.viaNearestOnSimplex True "model/demographic/test/casrTest" predictor_C filteredCensusTables_C
    DMC.checkCensusTables filteredCensusTables_C
-}
--    compareCSR_ASR cmdLine postInfo
    csrASR_ASER_Tracts 1000
--    asAE_Tracts
--    asAE_PUMAs
--    asAE_TractsFromPUMAs
--    asAE_average_stuff
--    DED.mapPE $ compareAS_AE cmdLine postInfo
--    DED.mapPE $ compareAS_AR cmdLine postInfo
--    asrASE_average_stuff
--    DED.mapPE $ compareASR_ASE' cmdLine postInfo
--    DED.mapPE $ compareASR_ASE cmdLine postInfo
{-    let amS = DED.averagingMatrix @(F.Record DMC.ASE) @(F.Record '[DT.Age5C, DT.Education4C]) F.rcast
        amE = DED.averagingMatrix @(F.Record DMC.ASE) @(F.Record DMC.AS) F.rcast
        amSE = DED.averagingMatrix @(F.Record DMC.ASE) @(F.Record '[DT.Age5C]) F.rcast
    K.logLE K.Info $ "amS=" <> toText (LA.dispf 2 amS)
    K.logLE K.Info $ "amE=" <> toText (LA.dispf 2 amE)
    K.logLE K.Info $ "amS x amE=" <> toText (LA.dispf 2 (amS LA.<> amE))
    K.logLE K.Info $ "amSE=" <> toText (LA.dispf 2 amSE)
-}
--    compareCSR_ASR_ASE cmdLine postInfo
--    compareCASR_ASE cmdLine postInfo
--    compareSER_ASR cmdLine postInfo
--    shiroData
  case resE of
    Right namedDocs →
      K.writeAllPandocResultsWithInfoAsHtml "" namedDocs
    Left err → putTextLn $ "Pandoc Error: " <> Pandoc.renderError err

type ShiroACS k = (k V.++ DMC.CASER V.++ '[DT.PopCount])
type ShiroMicro k = (k V.++ DMC.CASER V.++ '[ACS.PUMSWeight])

getRandomInt :: K.Member PR.RandomFu r => Int -> K.Sem r Int
getRandomInt max =
  PR.sampleRVar $ (R.uniform 0 max)

getRandomInts :: K.Member PR.RandomFu r => Int -> Int -> K.Sem r [Int]
getRandomInts n max =
  PR.sampleRVar $ replicateM n (R.uniform 0 max)


shuffle :: (K.KnitEffects r, K.Member PR.RandomFu r) => [a] -> K.Sem r [a]
shuffle l = go l [] where
  go [] xs = pure xs
  go ys xs = do
    sp <- getRandomInt $ (length ys - 1)
    let (lhl, t) = List.splitAt sp ys
    neT <- K.knitMaybe "shuffle: empty tail!" $ nonEmpty t
    let (s, mT) = NE.uncons neT
        rhl = maybe [] NE.toList mT
    go (lhl <> rhl) (s : xs)

randomIndices :: (K.KnitEffects r, K.Member PR.RandomFu r) => Int -> Int -> K.Sem r [Int]
randomIndices vecSize numIndices = take numIndices <$> shuffle [0..(vecSize - 1)]

randomIndices' :: (K.KnitEffects r, K.Member PR.RandomFu r) => Int -> Int -> K.Sem r [Int]
randomIndices' vecSize numIndices = getRandomInts numIndices (vecSize - 1)


shuffleNE :: (K.KnitEffects r, K.Member PR.RandomFu r) => NonEmpty a -> K.Sem r (NonEmpty a)
shuffleNE ne = do
  shuffled <- shuffle $ NE.toList ne
  shuffledNE <- K.knitMaybe "shuffleNE: empty list after shuffle!" $ nonEmpty shuffled
  pure shuffledNE

testHalf :: (K.KnitEffects r, K.Member PR.RandomFu r) => NonEmpty x -> K.Sem r (NonEmpty x, NonEmpty x)
testHalf ne = do
  let l = NE.toList ne
  shuffled <- shuffle l
  let n = length l
      sp = let m = n `div` 2 in if even n then m else m + 1
      (tr, te) = List.splitAt sp shuffled
  trNE <- K.knitMaybe "testHalf: Training split is empty" $ nonEmpty tr
  teNE <- K.knitMaybe "testHalf: Testing split is empty" $ nonEmpty te
  pure (trNE, teNE)

testOne :: (K.KnitEffects r, K.Member PR.RandomFu r) => NonEmpty x -> K.Sem r (NonEmpty x, NonEmpty x)
testOne ne = do
  shuffledNE <- shuffleNE ne
  let teNE = NE.singleton $ head shuffledNE
  trNE <- K.knitMaybe "testOne: Empty tail of input list" $ nonEmpty $ tail shuffledNE
  pure (trNE, teNE)

-- split within each outer, keeping inner together
splitGEOs :: forall a b rs r . (FSI.RecVec rs, Eq a, Eq b, K.KnitEffects r, K.Member PR.RandomFu r)
          => (forall x. NonEmpty x -> K.Sem r (NonEmpty x, NonEmpty x))
          -> (F.Record rs -> a)
          -> (F.Record rs -> b)
          -> F.FrameRec rs
          -> K.Sem r (F.FrameRec rs, F.FrameRec rs)
splitGEOs split outer inner rows = do
  let f :: [F.Record rs] -> K.Sem r (F.FrameRec rs, F.FrameRec rs)
      f = fmap reassemble . restructure . fmap partition . innersInOuters
      restructure :: [K.Sem r ([x], [x])] -> K.Sem r ([[x]], [[x]])
      restructure = fmap unzip . sequence
      groupBy :: Eq x => (y -> x) -> [y] -> [[y]]
      groupBy g = List.groupBy (\a b -> g a == g b)
      innersInOuters :: [F.Record rs] -> [[[F.Record rs]]]
      innersInOuters = fmap (groupBy inner) . groupBy outer
      reassemble :: ([[[F.Record rs]]],[[[F.Record rs]]]) -> (F.FrameRec rs, F.FrameRec rs)
      reassemble (a, b) = (F.toFrame $ mconcat $ mconcat a, F.toFrame $ mconcat $ mconcat b)
      partition :: [x] -> K.Sem r ([x], [x])
      partition l = do
        ne <- K.knitMaybe "splitGEOs.partition: Empty list given as input" $ nonEmpty l
        (trNE, teNE) <- split ne
        pure $ (NE.toList trNE, NE.toList teNE)
  f $ FL.fold FL.list rows


type PUMA_ASERToCsvR = [GT.StateAbbreviation, GT.PUMA, DT.Age5C, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]

pumaASERToCSV :: K.KnitEffects r => Text -> F.FrameRec PUMA_ASERToCsvR -> K.Sem r ()
pumaASERToCSV fileName rows = do
  let newHeaderMap = M.fromList [("StateAbbreviation", "state")
                                ,("PUMA","puma")
                                , ("PopCount", "pop_count")
                                , ("Age5C", "age_5")
                                , ("SexC", "sex_2")
                                , ("Education4C", "education_4")
                                , ("Race5C", "race_5")
                                ]
      formatRows :: V.Rec (V.Lift (->) V.ElField (V.Const Text)) PUMA_ASERToCsvR
      formatRows = FCSV.formatTextAsIs V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow
                   V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& V.RNil
  K.liftKnit @IO $ FCSV.writeLines ("../../forPhilip/" <> toString fileName)
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatRows ","
    $ FCSV.foldableToStream
    $ fmap F.rcast rows

type TractASEToCsvR = [GT.StateAbbreviation, GT.TractGeoId, DT.Age5C, DT.SexC, DT.Education4C, DT.PopCount]


tractASEToCSV :: K.KnitEffects r => Text -> F.FrameRec TractASEToCsvR -> K.Sem r ()
tractASEToCSV fileName rows = do
  let newHeaderMap = M.fromList [("StateAbbreviation", "state")
                                ,("TractGeoId","tractId")
                                , ("PopCount", "pop_count")
                                , ("Age5C", "age_5")
                                , ("SexC", "sex_2")
                                , ("Education4C", "education_4")
                                ]
      formatRows :: V.Rec (V.Lift (->) V.ElField (V.Const Text)) TractASEToCsvR
      formatRows = FCSV.formatTextAsIs V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow
                   V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& V.RNil
  K.liftKnit @IO $ FCSV.writeLines ("../../forMax/" <> toString fileName)
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatRows ","
    $ FCSV.foldableToStream
    $ fmap F.rcast rows




data AlphaCov = AlphaCov { acState :: Text, acPUMA :: Int, acAlphas :: [Double], acCovs :: [Double] }

alphaCovHeader :: Int -> Int -> Text
alphaCovHeader nAlpha nCov = "state_abbreviation, PUMA, " <> headers "alpha" nAlpha <> ", " <> headers "cov" nCov
  where
    headers t n = T.intercalate ", " $ fmap (\m -> t <> show m) [0..n-1]

alphaCovRow :: AlphaCov -> Text
alphaCovRow (AlphaCov sa p as cs) = T.intercalate ", " $ [sa, show p] <> fmap (toText @String . PF.printf "%2.2g") (as <> cs)


writeAlphaCovs :: Foldable f => Int -> Int -> FilePath -> f AlphaCov -> IO ()
writeAlphaCovs nAlpha nCov fp acs = sWriteTextLines fp (FSS.StreamlyStream s)
  where
    valueStream :: Streamly.Stream IO Text = fmap alphaCovRow $ FSS.stream $ sFromFoldable acs
    s :: Streamly.Stream IO Text = alphaCovHeader nAlpha nCov `Streamly.cons` valueStream

shiroData :: (K.KnitEffects r, BRCC.CacheEffects r) => K.Sem r ()
shiroData = do
  let wText = FCSV.formatTextAsIs
      wPrintf :: (V.KnownField t, V.Snd t ~ Double) => Int -> Int -> V.Lift (->) V.ElField (V.Const Text) t
      wPrintf n m = FCSV.liftFieldFormatter $ toText @String . PF.printf ("%" <> show n <> "." <> show n <> "g")
      formatPUMAWgts = FCSV.formatWithShow
                       V.:& FCSV.formatWithShow
                       V.:& FCSV.formatWithShow
                       V.:& wText V.:& FCSV.formatWithShow V.:& wPrintf 2 2 V.:& wPrintf 2 2 V.:& V.RNil
      newHeaderMap = M.fromList [("StateAbbreviation", "state")
                                ,("CongressionalDistrict","cd")
                                ,("PUMA","puma")
                                , ("PopCount", "pop_count")
                                , ("CitizenC", "citizen_2")
                                , ("Age5C", "age_5")
                                , ("SexC", "sex_2")
                                , ("Education4C", "education_4")
                                , ("Race5C", "race_5")
                                , ("Weight", "weight")
                                ,("StateFIPS","fips")
                                ,("Population2016","pop_count")
                                ,("FracCDInPUMA","frac_cd_in_puma")
                                ,("FracPUMAInCD","frac_puma_in_cd")
                                ]
      exampleF = id -- F.takeRows 100
  examplePUMAWgts <- K.ignoreCacheTimeM (fmap exampleF <$> BRDF.cdFromPUMA2012Loader 116)
  K.liftKnit @IO $ FCSV.writeLines "../forShiro/exPumaWgts.csv" $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatPUMAWgts "," $ FCSV.foldableToStream examplePUMAWgts
  let formatACSMicro = FCSV.formatWithShow V.:& wText V.:& FCSV.formatWithShow -- header
                     V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow -- cats
                     V.:& FCSV.formatWithShow V.:& V.RNil
      (srcWindow, cachedSrc) = ACS.acs1Yr2012_22
  exampleACSMicro <- K.ignoreCacheTimeM (fmap exampleF <$> DDP.cachedACSa5 srcWindow cachedSrc  2020)
  K.liftKnit @IO $ FCSV.writeLines "../forShiro/exACSMicro.csv"
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatACSMicro ","
    $ FCSV.foldableToStream
    $ fmap (F.rcast @(ShiroMicro [BRDF.Year,GT.StateAbbreviation,GT.PUMA]))
    $ exampleACSMicro
  let formatACSByPUMA = FCSV.formatWithShow V.:& wText V.:& FCSV.formatWithShow -- header
                        V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow -- cats
                        V.:& FCSV.formatWithShow V.:& V.RNil
  exampleACSByPUMA <- K.ignoreCacheTimeM (fmap exampleF <$> DDP.cachedACSa5ByPUMA srcWindow cachedSrc 2020)
  K.liftKnit @IO $ FCSV.writeLines "../forShiro/exACSByPuma.csv"
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatACSByPUMA ","
    $ FCSV.foldableToStream
    $ fmap (F.rcast @(ShiroACS [BRDF.Year,GT.StateAbbreviation,GT.PUMA])) exampleACSByPUMA
  let formatACSByCD = FCSV.formatWithShow V.:& wText V.:& FCSV.formatWithShow -- header
                      V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow V.:& FCSV.formatWithShow -- cats
                      V.:& FCSV.formatWithShow V.:& V.RNil
  exampleACSByCD <- K.ignoreCacheTimeM (fmap exampleF <$> DDP.cachedACSa5ByCD srcWindow cachedSrc 2020 Nothing)
  K.liftKnit @IO $ FCSV.writeLines "../forShiro/exACSByCD.csv"
    $ FCSV.streamSV' @_ @(StreamlyStream Stream) newHeaderMap formatACSByCD ","
    $ FCSV.foldableToStream
    $ fmap (F.rcast @(ShiroACS [BRDF.Year,GT.StateAbbreviation,GT.CongressionalDistrict])) exampleACSByCD

{-
type TestRowP ok ks = ok V.++ ks V.++ '[DT.PopCount]
averageDiffs :: forall qs outerK ks . (qs F.⊆ ks) => F.FrameRec (TestRow outerK ks) ->  F.FrameRec (TestRow outerK ks) -> F.FrameRec (TestRowP outerK ks)
averageDiffs products fulls = undefined where
  productsP = fmap F.rcast @(TestRowP outerKs ks) products
  fullsP = fmap F.rcast @(TestRowP outerKs ks) fulls
  innerFld :: FL.Fold (F.Record (as V.++ '[DT.PopCount])) (F.FrameRec (as V.++ '[DT.PopCount]))
  innerFld =
    let avgFld = FL.premap (view DT.popCount) FL.mean
        listFld = FL.list
        f avg = fmap (const avg `over` DT.popCount)
    in f <$> fmap round avgFld <*> FL.list
  avgFld :: FL.Fold (F.Record (ks V.++ '[DT.PopCount])) (F.FrameRec (ks V.++ '[DT.PopCount]))
-}


{-
originalPost ::  (K.KnitMany r, K.KnitEffects r, BRK.CacheEffects r) => BR.CommandLine -> BR.PostInfo -> K.Sem r ()
originalPost cmdLine postInfo = do
    K.logLE K.Info $ "Loading ACS data for each PUMA" <> show cmdLine
    acsA5ByPUMA_C <- DDP.cachedACSa5ByPUMA

    let msSER_A5SR_sd = DMS.reKeyMarginalStructure
                        (F.rcast @[DT.SexC, DT.Race5C, DT.Education4C, DT.Age5FC])
                        (F.rcast @A5SER)
                        $ DMS.combineMarginalStructuresF  @'[DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.Age5FC]
                        DTP.sumLens DMS.innerProductSum
                        (DMS.identityMarginalStructure DTP.sumLens)
                        (DMS.identityMarginalStructure DTP.sumLens)
{-
        msSER_A5SR_sd' = DMC.marginalStructure @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] DTP.sumLens DMS.innerProductSum
    let eqStencils = DMS.msStencils msSER_A5SR_sd == DMS.msStencils msSER_A5SR_sd'
    K.logLE K.Info $ "Stencils are == ?" <> show eqStencils
    when (not eqStencils) $ K.logLE K.Info $ "orig=" <> show (DMS.msStencils msSER_A5SR_sd) <> "\nnew=" <> show (DMS.msStencils msSER_A5SR_sd')
    K.knitError "STOP"
-}
    {-
    K.logLE K.Info "Building district-level ASER joint distributions"
    predictor_C <- DMC.predictorModel3 False cmdLine
    sld2022CensusTables_C <- BRC.censusTablesFor2022SLDs
    let censusTableCond r = r ^. GT.stateFIPS == 9 && r ^. GT.districtTypeC == GT.StateUpper -- && r ^. GT.districtName == "10"
        filteredCensusTables_C = fmap (BRC.filterCensusTables censusTableCond) sld2022CensusTables_C
    predictedSLDDemographics_C <- fmap fst $ DMC.predictedCensusASER True "model/demographic/test/svdBug" predictor_C filteredCensusTables_C
-}
    acsA5ByCD_C <- DDP.cachedACSa5ByCD
    acsA5ByCD <- K.ignoreCacheTime acsA5ByCD_C
    productNS3_SER_A5SR_C <- fmap snd <$>
                             testProductNS_CDs @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] False True "model/demographic/ser_a5sr" "SER_A5SR" cmdLine
                             (fmap (fmap F.rcast) acsA5ByPUMA_C)
                             (fmap (fmap F.rcast) acsA5ByCD_C)
    productNS3_SER_A5SR <- fmap (fmap F.rcast) $ K.ignoreCacheTime productNS3_SER_A5SR_C

    let vecToFrame ok ks v = fmap (\(k, c) -> ok F.<+> k F.<+> FT.recordSingleton @DT.PopCount (round c)) $ zip ks (VS.toList v)
        prodAndModeledToFrames ks (ok, pv, mv) = (vecToFrame ok ks pv, vecToFrame ok ks mv)


    let ms_SER_A5SR = DMC.marginalStructure @DMC.A5SER @'[DT.Age5FC] @'[DT.Education4C] DMS.cwdWgtLens DMS.innerProductCWD'
    nvps_SER_A5SR_C <- DMC.cachedNVProjections "model/demographic/ser_a5sr_NVPs.bin" ms_SER_A5SR acsA5ByPUMA_C
    nvps_SER_A5SR <- K.ignoreCacheTime nvps_SER_A5SR_C
    let cMatrix_SER_A5SR = DED.mMatrix (DMS.msNumCategories ms_SER_A5SR) (DMS.msStencils ms_SER_A5SR)
        cdPopMap = FL.fold (FL.premap (\r -> (DDP.districtKeyT r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) acsA5ByCD
        statePopMap = FL.fold (FL.premap (\r -> (view GT.stateAbbreviation r, r ^. DT.popCount)) $ FL.foldByKeyMap FL.sum) acsA5ByCD
        cdModelData1 = FL.fold
                       (DTM1.nullVecProjectionsModelDataFldCheck
                         msSER_A5SR_sd
                         nvps_SER_A5SR
                         (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                         (F.rcast @A5SER)
                         (realToFrac . view DT.popCount)
                         DTM1.a5serModelDatFld
                       )
                       acsA5ByCD

    K.logLE K.Info $ "Running model summary stats as covariates model, if necessary."
    let modelConfig = DTM1.ModelConfig nvps_SER_A5SR True
                      DTM1.designMatrixRowASER DTM1.AlphaHierNonCentered DTM1.NormalDist DTM1.aserModelFuncs
    res_C <- DTM1.runProjModel @A5SER False cmdLine (DTM1.RunConfig False False) modelConfig msSER_A5SR_sd DTM1.a5serModelDatFld
    modelRes <- K.ignoreCacheTime res_C

    forM_ cdModelData1 $ \(sar, md, nVpsActual, pV, nV) -> do
      let keyT = DDP.districtKeyT sar
          n = VS.sum pV

      when (BR.logLevel cmdLine >= BR.LogDebugMinimal) $ do
--        K.logLE K.Info $ keyT <> " nvps (actual) =" <> DED.prettyVector nVpsActual
        K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> " (" <> show (VS.sum nV) <> ")"
        K.logLE K.Info $ keyT <> " prod    counts=" <> DED.prettyVector pV <> " (" <> show (VS.sum pV) <> ")"
        K.logLE K.Info $ keyT <> " nvps counts   =" <> DED.prettyVector (DTP.applyNSPWeights nvps_SER_A5SR (VS.map (* n) nVpsActual) pV)
        K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector (cMatrix_SER_A5SR LA.#> (nV - pV))
        K.logLE K.Info $ keyT <> " predictors: " <> show md
        nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (sar ^. GT.stateAbbreviation) md)
        K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled
        let simplexFull = DTP.projectToSimplex $ DTP.applyNSPWeights nvps_SER_A5SR nvpsModeled (VS.map (/ VS.sum pV) pV)
            simplexNVPs = DTP.fullToProj nvps_SER_A5SR simplexFull
--        nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections 1e-4 nvpsModeled (VS.map (/ n) pV)
        K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs
        K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector (VS.map (* VS.sum pV) simplexFull)
--    K.knitError "STOP"

    let smcRowToProdAndModeled1 (ok, md, _, pV, _) = do
          let n = VS.sum pV --realToFrac $ DMS.msNumCategories marginalStructure
          nvpsModeled <-  VS.fromList <$> (K.knitEither $ DTM1.modelResultNVPs DTM1.aserModelFuncs modelRes (view GT.stateAbbreviation ok) md)
          let simplexFull = VS.map (* n) $ DTP.projectToSimplex $ DTP.applyNSPWeights nvps_SER_A5SR nvpsModeled (VS.map (/ n) pV)
--            simplexNVPs = DTP.fullToProj testProjections simplexFull
--          nvpsOptimal <- DED.mapPE $ DTP.optimalWeights testProjections 1e-4 nvpsModeled (VS.map (/ n) pV)
          let mV = simplexFull --DTP.applyNSPWeights testProjections (VS.map (* n) nvpsOptimal) pV
          pure (ok, pV, mV)
    prodAndModeled1 <- traverse smcRowToProdAndModeled1 cdModelData1
    let (product_SER_A5SR, productNS1_SER_A5SR) =
          first (F.toFrame . concat)
          $ second (F.toFrame . concat)
          $ unzip
          $ fmap (prodAndModeledToFrames (S.toList $ Keyed.elements @(F.Record A5SER))) prodAndModeled1
        addSimpleAge4 r = FT.recordSingleton @DT.SimpleAgeC (DT.age4ToSimple $ r ^. DT.age4C) F.<+> r
        addSimpleAge5 r = FT.recordSingleton @DT.SimpleAgeC (DT.age5FToSimple $ r ^. DT.age5FC) F.<+> r
        aggregateAge = FMR.concatFold
                       $ FMR.mapReduceFold
                       (FMR.Unpack $ \r -> [addSimpleAge5 r])
                       (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                         @'[DT.PopCount])
                       (FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum)
        product_SER_A2SR = FL.fold aggregateAge product_SER_A5SR
        productNS1_SER_A2SR = FL.fold aggregateAge productNS1_SER_A5SR
        productNS3_SER_A2SR = FL.fold aggregateAge productNS3_SER_A5SR

    K.logLE K.Info $ "Loading ACS data and building marginal distributions (SER, ASR, CSR) for each CD" <> show cmdLine
    let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
    let acsA5SampleWZ_C = fmap (FL.fold
                                (FMR.concatFold
                                 $ FMR.mapReduceFold
                                 FMR.noUnpack
                                 (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict]
                                  @[DT.CitizenC, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount, DT.PWPopPerSqMile])
                                 (FMR.foldAndLabel
                                  (Keyed.addDefaultRec @[DT.CitizenC, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C] zc)
                                  (\k r -> fmap (k F.<+>) r)
                                 )
                                )
                               )
                          $ acsA5ByCD_C
    let allStates = S.toList $ FL.fold (FL.premap (view GT.stateAbbreviation) FL.set) acsA5ByCD
        allCDs = S.toList $ FL.fold (FL.premap DDP.districtKey FL.set) acsA5ByCD
        emptyUnless x y = if x then y else mempty
        logText t k = Nothing --Just $ t <> ": Joint distribution matching for " <> show k
        zeroPopAndDens :: F.Record [DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
        tableMatchingDataFunctions = DED.TableMatchingDataFunctions zeroPopAndDens recToCWD cwdToRec cwdN updateCWDCount
--        exampleState = "KY"
        rerunMatches = True
    -- SER to A2SER
    let toOutputRow :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow = F.rcast
    let toOutputRow2 :: ([BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]  F.⊆ rs)
                    => F.Record rs -> F.Record [BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C, DT.PopCount]
        toOutputRow2 = F.rcast
    -- actual
    let acsSampleA5SER_C = F.toFrame
                           . FL.fold (FMR.concatFold
                                      $ FMR.mapReduceFold
                                      FMR.noUnpack
                                      (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
                                       @'[DT.PopCount, DT.PWPopPerSqMile])
                                      (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                     )
                           <$> acsA5SampleWZ_C
    acsSampleA5SER <- K.ignoreCacheTime acsSampleA5SER_C

    let acsSampleA2SER_C = F.toFrame
                           . FL.fold (FMR.concatFold
                                      $ FMR.mapReduceFold
                                      (FMR.Unpack $ \r -> [FT.recordSingleton @DT.SimpleAgeC (DT.age5FToSimple $ r ^. DT.age5FC) F.<+> r])
                                      (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
                                       @'[DT.PopCount, DT.PWPopPerSqMile])
                                      (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                     )
                           <$> acsA5SampleWZ_C
    acsSampleA2SER <- K.ignoreCacheTime acsSampleA2SER_C

    -- marginals, SER and CSR
    let acsSampleSER_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SexC, DT.Education4C, DT.Race5C]
                                     @'[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsA5SampleWZ_C
    let acsSampleASR_C = F.toFrame
                         . FL.fold (FMR.concatFold
                                    $ FMR.mapReduceFold
                                    FMR.noUnpack
                                    (FMR.assignKeysAndData @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Race5C]
                                      @[DT.PopCount, DT.PWPopPerSqMile])
                                    (FMR.foldAndAddKey DDP.aggregatePeopleAndDensityF)
                                   )
                         <$> acsA5SampleWZ_C

    -- model & match pipeline
    a2FromSER_C <- SM.runModel
                   True cmdLine (SM.ModelConfig () False SM.HCentered True)
                   ("Age", DT.age5FToSimple . view DT.age5FC)
                   ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)
{-
    cFromSER_C <- SM.runModel
                  False cmdLine (SM.ModelConfig () False SM.HCentered True)
                  ("Cit", view DT.citizenC)
                  ("SER", F.rcast @[DT.SexC, DT.Education4C, DT.Race5C], SM.dmrS_ER)
-}
    let a2srRec :: (DT.SimpleAge, DT.Sex, DT.Race5) -> F.Record [DT.SimpleAgeC, DT.SexC, DT.Race5C]
        a2srRec (c, s, r) = c F.&: s F.&: r F.&: V.RNil
        a2srDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                    (\r -> a2srRec (DT.age5FToSimple (r ^. DT.age5FC), r ^. DT.sexC, r ^. DT.race5C))
        a2srDS = getSum <<$>> FL.fold a2srDSFld acsA5ByCD

        serRec :: (DT.Sex, DT.Education4, DT.Race5) -> F.Record [DT.SexC, DT.Education4C, DT.Race5C]
        serRec (s, e, r) = s F.&: e F.&: r F.&: V.RNil
        serDSFld =  DED.desiredSumsFld
                    (Sum . F.rgetField @DT.PopCount)
                    (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict])
                    (\r -> serRec (r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C))
        serDS = getSum <<$>> FL.fold serDSFld acsA5ByCD

    let serToA2SER_PC eu = fmap
          (\m -> DED.pipelineStep @[DT.SexC, DT.Education4C, DT.Race5C] @'[DT.SimpleAgeC] @_ @_ @_ @DT.PopCount
                 tableMatchingDataFunctions
                 (DED.minConstrained DED.klPObjF)
                 (DED.enrichFrameFromBinaryModelF @DT.SimpleAgeC @DT.PopCount m (view GT.stateAbbreviation) DT.Under DT.EqualOrOver)
                 (logText "SER -> A2SER")
                 (emptyUnless eu
                   $ DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C] serDS
                   <> DED.desiredSumMapToLookup @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C] a2srDS)
          )
          a2FromSER_C

    modelA2SERFromSER <- fmap (fmap toOutputRow2)
                         $ BRCC.clearIf' rerunMatches  "model/synthJoint/serToA2SER_MO.bin" >>=
                         \ck -> K.ignoreCacheTimeM $ BRCC.retrieveOrMakeFrame ck
                                ((,) <$> serToA2SER_PC False <*> acsSampleSER_C)
                                $ \(p, d) -> DED.mapPE $ p d

    modelCO_A2SERFromSER <- fmap (fmap toOutputRow2)
                            $ BRCC.clearIf' rerunMatches  "model/synthJoint/serToA2SER.bin" >>=
                            \ck -> K.ignoreCacheTimeM $ BRCC.retrieveOrMakeFrame ck
                                   ((,) <$> serToA2SER_PC True <*> acsSampleSER_C)
                                   $ \(p, d) -> DED.mapPE $ p d

    synthModelPaths <- postPaths "SynthModel" cmdLine
    BRK.brNewPost synthModelPaths postInfo "SynthModel" $ do
      let showCellKeyA2 r = show (r ^. GT.stateAbbreviation, r ^. DT.simpleAgeC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
          edOrder =  show <$> S.toList (Keyed.elements @DT.Education4)
--          age4Order = show <$> S.toList (Keyed.elements @DT.Age4)
          age5Order = show <$> S.toList (Keyed.elements @DT.Age5F)
          simpleAgeOrder = show <$> S.toList (Keyed.elements @DT.SimpleAge)
      compareResults @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C]
        synthModelPaths postInfo False False cdPopMap DDP.districtKeyT (Just "NY")
        (F.rcast @[DT.SimpleAgeC, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.simpleAgeC, r ^. DT.education4C))
        showCellKeyA2
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("SimpleAge", show . view DT.simpleAgeC, Just simpleAgeOrder))
        (fmap toOutputRow2 acsSampleA2SER)
        [MethodResult (fmap toOutputRow2 acsSampleA2SER) (Just "Actual") Nothing Nothing
        ,MethodResult product_SER_A2SR (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult productNS1_SER_A2SR (Just "Null-Space_v1") (Just "Null-Space_v1") (Just "Null-space_v1")
        ,MethodResult productNS3_SER_A2SR (Just "Null-Space_v3") (Just "Null-Space_v3") (Just "Null-space_v3")
        ,MethodResult modelA2SERFromSER (Just "Model Only") (Just "Model Only") (Just "Model Only")
        ,MethodResult modelCO_A2SERFromSER (Just "Model+CO") (Just "Model+CO") (Just "Model+CO")
        ]

      let showCellKey r = show (r ^. GT.stateAbbreviation, r ^. DT.age5FC, r ^. DT.sexC, r ^. DT.education4C, r ^. DT.race5C)
      compareResults @[BRDF.Year, GT.StateAbbreviation, GT.CongressionalDistrict, DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C]
        synthModelPaths postInfo False False statePopMap (view GT.stateAbbreviation) (Just "NY")
        (F.rcast @[DT.Age5FC, DT.SexC, DT.Education4C, DT.Race5C])
        (\r -> (r ^. DT.sexC, r ^. DT.race5C))
        (\r -> (r ^. DT.age5FC, r ^. DT.education4C))
        showCellKey
        (Just ("Education", show . view DT.education4C, Just edOrder))
        (Just ("Age", show . view DT.age5FC, Just age5Order))
        (fmap toOutputRow acsSampleA5SER)
        [MethodResult (fmap toOutputRow acsSampleA5SER) (Just "Actual") Nothing Nothing
        ,MethodResult product_SER_A5SR (Just "Product") (Just "Marginal Product") (Just "Product")
        ,MethodResult productNS1_SER_A5SR (Just "Null-Space_v1") (Just "Null-Space_v1") (Just "Null-Space_v1")
        ,MethodResult productNS3_SER_A5SR (Just "Null-Space_v3") (Just "Null-Space_v3") (Just "Null-Space_v3")
        ]
    pure ()
-}

data ErrTableRow a = ErrTableRow { eTrLabel :: a, eTrErr :: [Double] }

-- This function expects the same length list in the table row as is provided in the col headders argument
errColonnadeFlex :: Foldable f => (Double -> Text) -> f Text -> Text -> C.Colonnade C.Headed (ErrTableRow Text) Text
errColonnadeFlex errFmt colHeaders regionName =
  C.headed regionName eTrLabel
  <> mconcat (fmap errCol [0..(length chList - 1)])
  where
    chList = FL.fold FL.list colHeaders
    errCol n = C.headed  (chList List.!! n) (errFmt . (List.!! n) . eTrErr)
--    cols n = errCol n

klColonnade :: C.Colonnade C.Headed (Text, Double, Double, Double, Double) Text
klColonnade =
  let sa (x, _, _, _, _) = x
      serProd (_, x, _, _, _) = x
      serProdNS (_, _, x, _, _) = x
      serModel (_, _, _, x, _) = x
      serModelCO (_, _, _, _, x) = x
      fmtX :: Double -> Text
      fmtX x = toText @String $ PF.printf "%2.2e" x
  in C.headed "State" sa
     <> C.headed "Product" (fmtX . serProd)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serProd)
     <> C.headed "Product + NS" (fmtX . serProdNS)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serProdNS)
     <> C.headed "Model Only" (fmtX . serModel)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serModel)
     <> C.headed "Model + Matching" (fmtX . serModelCO)
     <> C.headed "Error (%)" (toText @String . PF.printf "%2.0g" . (100 *) . sqrt . (2 *) . serModelCO)


mapColonnade :: (Show a, Show b, Ord b, Keyed.FiniteSet b) => C.Colonnade C.Headed (a, Map b Int) Text
mapColonnade = C.headed "" (show . fst)
               <> mconcat (fmap (\b -> C.headed (show b) (fixMaybe . M.lookup b . snd)) (S.toAscList $ Keyed.elements))
               <>  C.headed "Total" (fixMaybe . sumM) where
  fixMaybe = maybe "0" show
  sumM x = fmap (FL.fold FL.sum) <$> traverse (\b -> M.lookup b (snd x)) $ S.toList $ Keyed.elements


-- emptyRel = [Path.reldir||]
postDir ∷ Path.Path Rel Dir
postDir = [Path.reldir|br-2022-Demographics/posts|]

postLocalDraft
  ∷ Path.Path Rel Dir
  → Maybe (Path.Path Rel Dir)
  → Path.Path Rel Dir
postLocalDraft p mRSD = case mRSD of
  Nothing → postDir BR.</> p BR.</> [Path.reldir|draft|]
  Just rsd → postDir BR.</> p BR.</> rsd

postInputs ∷ Path.Path Rel Dir → Path.Path Rel Dir
postInputs p = postDir BR.</> p BR.</> [Path.reldir|inputs|]

sharedInputs ∷ Path.Path Rel Dir
sharedInputs = postDir BR.</> [Path.reldir|Shared|] BR.</> [Path.reldir|inputs|]

postOnline ∷ Path.Path Rel t → Path.Path Rel t
postOnline p = [Path.reldir|research/Demographics|] BR.</> p

postPaths
  ∷ (K.KnitEffects r)
  ⇒ Text
  → BR.CommandLine
  → K.Sem r (BR.PostPaths BR.Abs)
postPaths t cmdLine = do
  let mRelSubDir = case cmdLine of
        BR.CLLocalDraft _ _ mS _ → maybe Nothing BR.parseRelDir $ fmap toString mS
        _ → Nothing
  postSpecificP ← K.knitEither $ first show $ Path.parseRelDir $ toString t
  BR.postPaths
    BR.defaultLocalRoot
    sharedInputs
    (postInputs postSpecificP)
    (postLocalDraft postSpecificP mRelSubDir)
    (postOnline postSpecificP)

logLengthC :: (K.KnitEffects r, Foldable f) => K.ActionWithCacheTime r (f a) -> Text -> K.Sem r ()
logLengthC xC t = K.ignoreCacheTime xC >>= \x -> K.logLE K.Info $ t <> " has " <> show (FL.fold FL.length x) <> " rows."

distCompareChart :: (Ord k, Show k, Show a, Ord a, K.KnitEffects r)
                 => BR.PostPaths Path.Abs
                 -> BR.PostInfo
                 -> Text
                 -> FV.ViewConfig
                 -> Text
                 -> (F.Record rs -> k) -- key for map
                 -> (k -> Text) -- description for tooltip
                 -> Bool
                 -> Maybe (Text, k -> Text, Maybe [Text]) -- category for color
                 -> Maybe (Text, k -> Text, Maybe [Text]) -- category for shape
                 -> Bool
                 -> (F.Record rs -> Double)
                 -> Maybe (k -> a, Map a Int) -- pop counts to divide by
                 -> (Text, F.FrameRec rs)
                 -> (Text, F.FrameRec rs)
                 -> K.Sem r GV.VegaLite
distCompareChart pp pi' chartDataPrefix vc title key keyText facetB cat1M cat2M asDiffB count scalesM (actualLabel, actualRows) (synthLabel, synthRows) = do
  let assoc r = (key r, Sum $ count r)
      toMap = FL.fold (fmap getSum <$> FL.premap assoc FL.map)
      whenMatchedF _ aCount sCount = Right (aCount, sCount)
      whenMatched = MM.zipWithAMatched whenMatchedF
      whenMissingF t k _ = Left $ "Missing key=" <> show k <> " in " <> t <> " rows."
      whenMissingFromX = MM.traverseMissing (whenMissingF actualLabel)
      whenMissingFromY = MM.traverseMissing (whenMissingF synthLabel)
  mergedMap <- K.knitEither $ MM.mergeA whenMissingFromY whenMissingFromX whenMatched (toMap actualRows) (toMap synthRows)
--  let mergedList = (\(r1, r2) -> (key r1, (count r1, count r2))) <$> zip (FL.fold FL.list xRows) (FL.fold FL.list yRows)
  let diffLabel = actualLabel <> "-" <> synthLabel
      scaleErr :: Show a => a -> Text
      scaleErr a = "distCompareChart: Missing key=" <> show a <> " in scale lookup."
      scaleF = fmap realToFrac <$> case scalesM of
        Nothing -> const $ pure 1
        Just (keyF, scaleMap) -> \k -> let a = keyF k in maybeToRight (scaleErr a) $ M.lookup a scaleMap
  let rowToDataM (k, (xCount, yCount)) = do
        scale <- scaleF k
        pure $
          [ (actualLabel, GV.Number $ xCount / scale)
          , (synthLabel, GV.Number $ yCount / scale)
          , (diffLabel,  GV.Number $ (xCount - yCount) / scale)
          , ("Description", GV.Str $ keyText k)
          ]
          <> maybe [] (\(l, f, _) -> [(l, GV.Str $ f k)]) cat1M
          <> maybe [] (\(l, f, _) -> [(l, GV.Str $ f k)]) cat2M
  jsonRows <- K.knitEither $ FL.foldM (VJ.rowsToJSONM rowToDataM [] Nothing) (M.toList mergedMap)
  jsonFilePrefix <- K.getNextUnusedId $ "distCompareChart_" <> chartDataPrefix
  jsonUrl <- BRK.brAddJSON pp pi' jsonFilePrefix jsonRows
--      toVLDataRowsM x = GV.dataRow <$> rowToDataM x <*> pure []
--  vlData <- GV.dataFromRows [] . concat <$> (traverse toVLDataRowsM $ M.toList mergedMap)
  let xLabel = if asDiffB then synthLabel else actualLabel
      yLabel =  if asDiffB then diffLabel else synthLabel
  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      encX = GV.position GV.X [GV.PName xLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      encY = GV.position GV.Y [GV.PName yLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      encXYLineY = GV.position GV.Y [GV.PName actualLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      orderIf sortT = maybe [] (pure . sortT . pure . GV.CustomSort . GV.Strings)
      encColor = maybe id (\(l, _, mOrder) -> GV.color ([GV.MName l, GV.MmType GV.Nominal] <> orderIf GV.MSort mOrder)) cat1M
      encShape = maybe id (\(l, _, mOrder) -> GV.shape ([GV.MName l, GV.MmType GV.Nominal] <> orderIf GV.MSort mOrder)) cat2M
      encTooltips = GV.tooltips $ [ [GV.TName xLabel, GV.TmType GV.Quantitative]
                                  , [GV.TName yLabel, GV.TmType GV.Quantitative]
                                  , maybe [] (\(l, _, _) -> [GV.TName l, GV.TmType GV.Nominal]) cat1M
                                  , maybe [] (\(l, _, _) -> [GV.TName l, GV.TmType GV.Nominal]) cat2M
                                  , [GV.TName "Description", GV.TmType GV.Nominal]
                                  ]
      mark = GV.mark GV.Point [GV.MSize 1]
      enc = GV.encoding . encX . encY . if facetB then id else encColor . encShape . encTooltips
      markXYLine = GV.mark GV.Line [GV.MColor "black", GV.MStrokeDash [5,3]]
      dataSpec = GV.asSpec [enc [], mark]
      xySpec = GV.asSpec [(GV.encoding . encX . encXYLineY) [], markXYLine]
      layers = GV.layer $ [dataSpec] <> if asDiffB then [] else [xySpec]
      facets = GV.facet $ (maybe [] (\(l, _, mOrder) -> [GV.RowBy ([GV.FName l, GV.FmType GV.Nominal] <> orderIf GV.FSort mOrder)]) cat1M)
                           <> (maybe [] (\(l, _, mOrder) -> [GV.ColumnBy ([GV.FName l, GV.FmType GV.Nominal] <> orderIf GV.FSort mOrder)]) cat2M)

  pure $ if facetB
         then FV.configuredVegaLite vc [FV.title title, facets, GV.specification (GV.asSpec [layers]), vlData]
         else FV.configuredVegaLite vc [FV.title title, layers, vlData]


errorCompareHistogram :: K.KnitEffects r
                      => BR.PostPaths Path.Abs
                      -> BR.PostInfo
                      -> Text
                      -> Text
                      -> FV.ViewConfig
                      -> Text
                      -> [Text]
                      -> (Text, Double -> Double)
                      -> [ErrTableRow Text]
                      -> K.Sem r GV.VegaLite
errorCompareHistogram postPaths postInfo title chartID vc regionName labels (errLabel, errScale) tableRows = do
  let n = length labels
      colData k (ErrTableRow l es) = [("Source", GV.Str $ labels List.!! k)
                                    , (regionName, GV.Str l )
                                    , (errLabel, GV.Number $ errScale (es List.!! k))]
      kltrToData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]

      jsonRows = FL.fold (VJ.rowsToJSON' kltrToData [] Nothing) tableRows
  jsonFilePrefix <- K.getNextUnusedId $ ("errorCompareChart_" <> chartID)
  jsonUrl <-  BRK.brAddJSON postPaths postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      enc = GV.encoding
        . GV.position GV.X [GV.PName errLabel, GV.PmType GV.Quantitative, GV.PBin [GV.Step 1], GV.PAxis [GV.AxTitle errLabel]]
        . GV.position GV.Y [GV.PName errLabel, GV.PAggregate GV.Count, GV.PmType GV.Quantitative, GV.PAxis [GV.AxTitle $ "# " <> regionName]]
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]

  pure $ FV.configuredVegaLite vc [FV.title title
                                  , GV.mark GV.Line []
                                  , enc []
                                  , vlData
                                  ]

errorCompareScatter :: K.KnitEffects r
                  => BR.PostPaths Path.Abs
                  -> BR.PostInfo
                  -> Text
                  -> Text
                  -> FV.ViewConfig
                  -> Text
                  -> [Text]
                  -> (Text, Double -> Double)
                  -> [ErrTableRow Int]
                  -> K.Sem r GV.VegaLite
errorCompareScatter postPaths' postInfo title chartID vc unitName labels (errLabel', errScale) tableRows = do
  let n = length labels
      colData k (ErrTableRow n' es) = [("Source", GV.Str $ labels List.!! k)
                                    , (unitName, GV.Number $ realToFrac n')
                                    , (errLabel', GV.Number $ errScale (es List.!! k))]
      kltrToData kltr = fmap ($ kltr) $ fmap colData [0..(n-1)]

      jsonRows = FL.fold (VJ.rowsToJSON' kltrToData [] Nothing) tableRows
  jsonFilePrefix <- K.getNextUnusedId $ ("errorCompareChart_" <> chartID)
  jsonUrl <-  BRK.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
      enc = GV.encoding
        . GV.position GV.X [GV.PName unitName, GV.PmType GV.Quantitative]
        . GV.position GV.Y [GV.PName errLabel', GV.PmType GV.Quantitative]
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]

  pure $ FV.configuredVegaLite vc [FV.title title
                                  , GV.mark GV.Point [GV.MSize 5]
                                  , enc []
                                  , vlData
                                  ]

errorCompareXYScatter :: K.KnitEffects r
                      => BR.PostPaths Path.Abs
                      -> BR.PostInfo
                      -> Text
                      -> Text
                      -> FV.ViewConfig
                      -> Text
                      -> [Text]
                      -> (Text, Double -> Double)
                      -> [ErrTableRow Text]
                      -> K.Sem r GV.VegaLite
errorCompareXYScatter postPaths' postInfo title chartID vc regionName labels (errLabel, errScale) tableRows = do
  let n = length labels
  refLabel <- K.knitMaybe "errorCompareXYScatter: empty list of labels!" $ viaNonEmpty head labels
--      (xLabel : yLabel : _) = labels
  let colData k (ErrTableRow l es)
        = [(refLabel, GV.Number $ errScale (es List.!! 0))
          , ("Source", GV.Str $ labels List.!! k)
          , (regionName, GV.Str  l)
          , (errLabel, GV.Number $ errScale (es List.!! k))
          ]
      kltrToData kltr = fmap ($ kltr) $ fmap colData [1..(n-1)]
      jsonRows = FL.fold (VJ.rowsToJSON' kltrToData [] Nothing) tableRows
  jsonFilePrefix <- K.getNextUnusedId $ ("errorCompareXYScatter_" <> chartID)
  jsonUrl <-  BRK.brAddJSON postPaths' postInfo jsonFilePrefix jsonRows

  let vlData = GV.dataFromUrl jsonUrl [GV.JSON "values"]
--      transf = GV.transform . GV.pivot "Source" errLabel [GV.PiGroupBy [regionName]]
      encScatter = GV.encoding
        . GV.position GV.X [GV.PName refLabel, GV.PmType GV.Quantitative]
        . GV.position GV.Y [GV.PName errLabel, GV.PmType GV.Quantitative]
        . GV.color [GV.MName "Source", GV.MmType GV.Nominal]
      markScatter = GV.mark GV.Point [GV.MSize 0.3, GV.MOpacity 0.8]
      scatterSpec = GV.asSpec [encScatter [], markScatter]
      encXYLine = GV.encoding
                  . GV.position GV.Y [GV.PName refLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
                  . GV.position GV.X [GV.PName refLabel, GV.PmType GV.Quantitative, GV.PNoTitle]
      markXYLine = GV.mark GV.Line [GV.MColor "black", GV.MSize 0.5, GV.MFillOpacity 0.5]
      xyLineSpec = GV.asSpec [encXYLine [], markXYLine]
      layers = GV.layer [scatterSpec, xyLineSpec]
  pure $ FV.configuredVegaLite vc [FV.title title
                                  , layers
--                                  , transf []
                                  , vlData
                                  ]

  {-

-- NB: as ks - first component and bs is ks - second
-- E.g. if you are combining ASR and SER to make ASER ks=ASER, as=E, bs=A
testProductNS_CDs :: forall  ks (as :: [(Symbol, Type)]) (bs :: [(Symbol, Type)]) qs r .
                     (K.KnitEffects r, BRK.CacheEffects r
                     , qs ~ F.RDeleteAll (as V.++ bs) ks
                     , qs V.++ (as V.++ bs) ~ (qs V.++ as) V.++ bs
                     , Ord (F.Record ks)
                     , Keyed.FiniteSet (F.Record ks)
                     , ((qs V.++ as) V.++ bs) F.⊆ ks
                     , ks F.⊆ ((qs V.++ as) V.++ bs)
                     , Ord (F.Record qs)
                     , Ord (F.Record as)
                     , Ord (F.Record bs)
                     , Ord (F.Record (qs V.++ as))
                     , Ord (F.Record (qs V.++ bs))
                     , Ord (F.Record ((qs V.++ as) V.++ bs))
                     , Keyed.FiniteSet (F.Record qs)
                     , Keyed.FiniteSet (F.Record as)
                     , Keyed.FiniteSet (F.Record bs)
                     , Keyed.FiniteSet (F.Record (qs V.++ as))
                     , Keyed.FiniteSet (F.Record (qs V.++ bs))
                     , Keyed.FiniteSet (F.Record ((qs V.++ as) V.++ bs))
                     , as F.⊆ (qs V.++ as)
                     , bs F.⊆ (qs V.++ bs)
                     , qs F.⊆ (qs V.++ as)
                     , qs F.⊆ (qs V.++ bs)
                     , (qs V.++ as) F.⊆ ((qs V.++ as) V.++ bs)
                     , (qs V.++ bs) F.⊆ ((qs V.++ as) V.++ bs)
                     , qs V.++ as F.⊆ DMC.PUMARowR ks
                     , qs V.++ bs F.⊆ DMC.PUMARowR ks
                     , ks F.⊆ DMC.PUMARowR ks
                     , V.RMap as
                     , V.ReifyConstraint Show F.ElField as
                     , V.RecordToList as
                     , V.RMap bs
                     , V.ReifyConstraint Show F.ElField bs
                     , V.RecordToList bs
                     , F.ElemOf (DMC.KeysWD ks) DT.PopCount
                     , F.ElemOf (DMC.KeysWD ks) DT.PWPopPerSqMile
                     , ks F.⊆ CDRow ks
                     , qs V.++ as F.⊆ CDRow ks
                     , qs V.++ bs F.⊆ CDRow ks
                     , FSI.RecVec (DMC.KeysWD ks)
                     , V.RMap (DMC.KeysWD ks)
                     , FS.RecFlat (DMC.KeysWD ks)
                     )
                  => (forall k . DTP.NullVectorProjections k -> VS.Vector Double -> VS.Vector Double -> K.Sem r (VS.Vector Double))
                  -> Bool
                  -> Bool
                  -> Text
                  -> Text
                  -> BR.CommandLine
                  -> K.ActionWithCacheTime r (F.FrameRec (DMC.PUMARowR ks))
                  -> K.ActionWithCacheTime r (F.FrameRec (CDRow ks))
                  -> K.Sem r (K.ActionWithCacheTime r (F.FrameRec (CDRow ks), F.FrameRec (CDRow ks)))
testProductNS_CDs onSimplexM rerunModel clearCaches cachePrefix modelId cmdLine byPUMA_C byCD_C = do
  let productFrameCacheKey = cachePrefix <> "_productFrame.bin"
  when clearCaches $ traverse_ BRK.clearIfPresentD [productFrameCacheKey]
  let modelCacheDirE = (if rerunModel then Left else Right) cachePrefix
  (predictor_C, nvps_C, ms) <- DMC.predictorModel3 @as @bs @ks @qs modelCacheDirE modelId cmdLine byPUMA_C
  byCD <- K.ignoreCacheTime byCD_C
  nvps <- K.ignoreCacheTime nvps_C
  let cdModelData = FL.fold
        (DTM3.nullVecProjectionsModelDataFldCheck
         DMS.cwdWgtLens
         ms
         nvps
         (F.rcast @'[BRDF.Year, GT.StateAbbreviation, GT.StateFIPS, GT.CongressionalDistrict])
         (F.rcast @ks)
         DTM3.cwdF
         (DMC.innerFoldWD @(qs V.++ as) @(qs V.++ bs) @(CDRow ks) (F.rcast @(qs V.++ as)) (F.rcast @(qs V.++ bs)))
        )
        byCD
      productFrameDeps = (,) <$> nvps_C <*> predictor_C
  BRK.retrieveOrMake2Frames productFrameCacheKey productFrameDeps $ \(nvps', predictor) -> do
    let vecToFrame ok kws = fmap (\(k, w) -> ok F.<+> k F.<+> DMS.cwdToRec w) kws
        prodAndModeledToFrames (ok, pKWs, mKWs) = (vecToFrame ok pKWs, vecToFrame ok mKWs)

        smcRowToProdAndModeled (ok, covariates, _, pKWs, _) = do
--          let pV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) pKWs
          mKWs <- DTM3.predictedJoint onSimplexM DMS.cwdWgtLens predictor (ok ^. GT.stateAbbreviation) covariates pKWs
          pure (ok, pKWs, mKWs)

    let cMatrix = DED.mMatrix (DMS.msNumCategories ms) (DMS.msStencils ms)

    forM_ cdModelData $ \(sar, md, nVpsActual, pKWs, oKWs) -> do
      let keyT = DDP.districtKeyT sar
          pV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) pKWs
          nV = VS.fromList $ fmap (view (_2 . DMS.cwdWgtLens)) oKWs
          n = VS.sum pV

      when (BR.logLevel cmdLine >= BR.LogDebugMinimal) $ do
        let showSum v = " (" <> show (VS.sum v) <> ")"
        K.logLE K.Info $ keyT <> " actual  counts=" <> DED.prettyVector nV <> showSum nV
        K.logLE K.Info $ keyT <> " prod counts=" <> DED.prettyVector pV <> showSum pV
--        let nvpsV = DTP.applyNSPWeights nvps' (VS.map (* n) nVpsActual) pV
        K.logLE K.Info $ keyT <> "NS projections=" <> DED.prettyVector nVpsActual
        K.logLE K.Info $ "sumSq(projections)=" <> show (VS.sum $ VS.zipWith (*) nVpsActual nVpsActual)
        let cCheckV = cMatrix LA.#> (nV - pV)
        K.logLE K.Info $ keyT <> " C * (actual - prod) =" <> DED.prettyVector cCheckV <> showSum cCheckV
        K.logLE K.Info $ keyT <> " predictors: " <> DED.prettyVector md
        nvpsModeled <- VS.fromList <$> (K.knitEither $ DTM3.modelResultNVPs predictor (view GT.stateAbbreviation sar) md)
        K.logLE K.Info $ keyT <> " modeled  =" <> DED.prettyVector nvpsModeled <> showSum nvpsModeled
        let dNVPs = VS.zipWith (-) nVpsActual nvpsModeled
        K.logLE K.Info $ "dNVPS=" <> DED.prettyVector dNVPs
        K.logLE K.Info $ "sumSq(dNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dNVPs dNVPs)
        simplexFull <- onSimplexM nvps' nvpsModeled $ VS.map (/ n) pV
--        let simplexFull = DTP.projectToSimplex $ DTP.applyNSPWeights nvps' nvpsModeled (VS.map (/ n) pV)
        let simplexNVPs = DTP.fullToProj nvps' simplexFull
            dSimplexNVPs = VS.zipWith (-) simplexNVPs nVpsActual
        K.logLE K.Info $ "dSimplexNVPS=" <> DED.prettyVector dSimplexNVPs
        K.logLE K.Info $ "sumSq(dSimplexNVPS)=" <>  show (VS.sum $ VS.zipWith (*) dSimplexNVPs dSimplexNVPs)
        K.logLE K.Info $ keyT <> " onSimplex=" <> DED.prettyVector simplexNVPs <> showSum simplexNVPs
        let modeledCountsV = VS.map (* n) simplexFull
        K.logLE K.Info $ keyT <> " modeled counts=" <> DED.prettyVector modeledCountsV <> showSum modeledCountsV

    prodAndModeled <- traverse smcRowToProdAndModeled cdModelData
    pure $ bimap (F.toFrame . concat) (F.toFrame . concat)
         $ unzip
         $ fmap prodAndModeledToFrames prodAndModeled
-}

{-
runCitizenModel :: (K.KnitEffects r, BRK.CacheEffects r)
                => Bool
                -> BR.CommandLine
                -> SM.ModelConfig ()
                -> DM.DesignMatrixRow (F.Record [DT.SexC, DT.Education4C, DT.Race5C])
                -> K.Sem r (SM.ModelResult Text [DT.SexC, DT.Education4C, DT.Race5C])
runCitizenModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/citizen/" in if clearCaches then Left k else Right k
      dataName = "acsCitizen_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanCitizen"
                         ("normalSER_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      _postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  _ageModelPaths <- postPaths "CitizenModel" cmdLine
  acs_C <- DDP.cachedACSByState
--  K.ignoreCacheTime acs_C >>= BRK.logFrame
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateCitizenMN acs_C
      mcWithId = "normal" <$ mc
--  K.ignoreCacheTime acsMN_C >>= print
  logLengthC acsMN_C "acsByStateMNCit"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc)
  res <- do
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "citizenModel run complete."
  pure res


runAgeModel :: (K.KnitEffects r, BRK.CacheEffects r)
            => Bool
            -> BR.CommandLine
            -> SM.ModelConfig ()
            -> DM.DesignMatrixRow (F.Record [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
            -> K.Sem r (SM.ModelResult Text [DT.CitizenC, DT.SexC, DT.Education4C, DT.Race5C])
runAgeModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/age/" in if clearCaches then Left k else Right k
      dataName = "acsAge_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanAge"
                         ("normalSER_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      _postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  _ageModelPaths <- postPaths "AgeModel" cmdLine
  acs_C <- DDP.cachedACSByState
--  K.ignoreCacheTime acs_C >>= BRK.logFrame
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateAgeMN acs_C
      mcWithId = "normal" <$ mc
--  K.ignoreCacheTime acsMN_C >>= print
  logLengthC acsMN_C "acsByStateMNAge"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc) --(SM.designMatrixRowAge SM.logDensityDMRP))
  res <- do
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "ageModel run complete."
  pure res

runEduModel :: (K.KnitMany r, BRK.CacheEffects r)
            => Bool
            -> BR.CommandLine
            → SM.ModelConfig ()
            -> DM.DesignMatrixRow (F.Record [DT.CitizenC, DT.SexC, DT.Age4C, DT.Race5C])
            -> K.Sem r (SM.ModelResult Text [DT.CitizenC, DT.SexC, DT.Age4C, DT.Race5C])
runEduModel clearCaches cmdLine mc dmr = do
  let cacheDirE = let k = "model/demographic/edu/" in if clearCaches then Left k else Right k
      dataName = "acsEdu_" <> DM.dmName dmr <> SM.modelConfigSuffix mc
      runnerInputNames = SC.RunnerInputNames
                         "br-2022-Demographics/stanEdu"
                         ("normalSAR_" <> DM.dmName dmr <> SM.modelConfigSuffix mc)
                         (Just $ SC.GQNames "pp" dataName)
                         dataName
      only2020 r = F.rgetField @BRDF.Year r == 2020
      postInfo = BR.PostInfo (BR.postStage cmdLine) (BR.PubTimes BR.Unpublished Nothing)
  eduModelPaths <- postPaths "EduModel" cmdLine
  acs_C <- DDP.cachedACSByState
  logLengthC acs_C "acsByState"
  let acsMN_C = fmap DDP.acsByStateEduMN acs_C
  logLengthC acsMN_C "acsByStateMNEdu"
  states <- FL.fold (FL.premap (view GT.stateAbbreviation . fst) FL.set) <$> K.ignoreCacheTime acsMN_C
  (dw, code) <- SMR.dataWranglerAndCode acsMN_C (pure ())
                (SM.groupBuilderState (S.toList states))
                (SM.normalModel (contramap F.rcast dmr) mc) -- (Just SM.logDensityDMRP)
  let mcWithId = "normal" <$ mc
  acsMN <- K.ignoreCacheTime acsMN_C
  BRK.brNewPost eduModelPaths postInfo "EduModel" $ do
    _ <- K.addHvega Nothing Nothing $ chart (FV.ViewConfig 100 500 5) acsMN
    pure ()
  res <- do
    K.logLE K.Info "here"
    K.ignoreCacheTimeM
      $ SMR.runModel' @BRK.SerializerC @BRK.CacheData
      cacheDirE
      (Right runnerInputNames)
      dw
      code
      (SM.stateModelResultAction mcWithId dmr)
      (SMR.Both [SR.UnwrapNamed "successes" "yObserved"])
      acsMN_C
      (pure ())
  K.logLE K.Info "eduModel run complete."
--  K.logLE K.Info $ "result: " <> show res
  pure res
-}

chart :: Foldable f => FV.ViewConfig -> f DDP.ACSByStateEduMN -> GV.VegaLite
chart vc rows =
  let total v = v VU.! 0 + v VU.! 1
      grads v = v VU.! 1
      rowToData (r, v) = [("Sex", GV.Str $ show $ F.rgetField @DT.SexC r)
                         , ("State", GV.Str $ F.rgetField @GT.StateAbbreviation r)
                         , ("Age", GV.Str $ show $ F.rgetField @DT.Age6C r)
                         , ("Race", GV.Str $ show (F.rgetField @DT.Race5C r))
                         , ("Total", GV.Number $ realToFrac $ total v)
                         , ("Grads", GV.Number $ realToFrac $ grads v)
                         , ("FracGrad", GV.Number $ realToFrac (grads v) / realToFrac (total v))
                         ]
      toVLDataRows x = GV.dataRow (rowToData x) []
      vlData = GV.dataFromRows [] $ concat $ fmap toVLDataRows $ FL.fold FL.list rows
      encAge = GV.position GV.X [GV.PName "Age", GV.PmType GV.Nominal]
      encSex = GV.color [GV.MName "Sex", GV.MmType GV.Nominal]
      _encState = GV.row [GV.FName "State", GV.FmType GV.Nominal]
      encRace = GV.column [GV.FName "Race", GV.FmType GV.Nominal]
      encFracGrad = GV.position GV.Y [GV.PName "FracGrad", GV.PmType GV.Quantitative]
      encTotal = GV.size [GV.MName "Total", GV.MmType GV.Quantitative]
      mark = GV.mark GV.Circle []
      enc = (GV.encoding . encAge . encSex . encFracGrad . encRace . encTotal)
  in FV.configuredVegaLite vc [FV.title "FracGrad v Age", enc [], mark, vlData]


{-
applyMethods :: forall outerKs startKs addKs r .
                  (K.KnitEffects r
                  )
             -> Either Text Text
             -> (Bool -> K.ActionWithCacheTime r
                 (InputFrame (outerKs V.++ startKs) -> K.Sem r (ResultFrame (outerKs V.++ startKs V.++ addKs))) -- model/model+match pipeline
             -> K.ActionWithCacheTime r (InputFrame outerKs startKs addKs)
             -> K.Sem r (MethodResults outerKs startKs addKs)
applyMethods cacheKeyE mmPipelineF_C actual_C = do
  -- make sure we have zeroes
  let zc :: F.Record '[DT.PopCount, DT.PWPopPerSqMile] = 0 F.&: 0 F.&: V.RNil
      actualWZ_C =  fmap (FL.fold
                           (FMR.concatFold
                            $ FMR.mapReduceFold
                            (FMR.noUnpack)
                            (FMR.assignKeysAndData @outerKs @(startKs V.++ addKs V.++ [DT.PopCount, DT.PWPerSqMile])
                              (FMR.foldAndLabel
                                (Keyed.addDefaultRec @(startKs V.++ addKs) zc)
                                (\k r -> fmap (k F.<+>) r)
                              )
                            )
                           )
                         )
                    actual_C
  (rerun, cKey) <- case cacheKeyE of
    Left k -> (True, k)
    Right k -> (False, k)
  let modelOnlyDeps = (,) <$> mmPipelineF_C False, <*> actualWZ_C
      modelMatchDeps = (,) <$> mmPipelineF_C True, <*> actualWZ_C

  modelOnly_C <- BRK.clearIf' rerun cKey >>=
                 \ck -> BRK.retrieveOrMakeFrame ck modelOnlyDeps $ \(pF, inputData) -> DED.mapPE $ pF inputData
  modelMatch_C <- BRK.clearIf' rerun cKey >>=
                  \ck -> BRK.retrieveOrMakeFrame ck modelMatchDeps $ \(pF, inputData) -> DED.mapPE $ pF inputData
  product <-  DED.mapPE
              $ DTP.frameTableProduct @[BRDF.Year, GT.StateAbbreviation, DT.SexC, DT.Race5C] @'[DT.Education4C] @'[DT.CitizenC] @DT.PopCount
              (fmap F.rcast acsSampleSER) (fmap F.rcast acsSampleCSR)

-}
