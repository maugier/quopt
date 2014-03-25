module QueryOptimizer where

import Data.List (minimumBy)
import Data.Ord (comparing)

tSeek = 0.01
tRead = 0.0001
indexDepth = 4
pageSize = 1024

divUp q d = 0 - (q `div` (-d))

cost :: Node -> Double
cost n = fromIntegral (readCost n) * tRead +
         fromIntegral (seekCost n) * tSeek

data Field = Field { fieldName :: String
                   , fieldWidth :: Int }

instance Show Field where
	show = fieldName

data Table = Table String Int [Field]

data Node = Scan Table
          | Join JoinType Node Node
          | Select Int Node
          | Project [Field] Node
	  | Index Table Int

data JoinType = BNLJoin Int
              | HashJoin Int
              | MergeJoin Int Int
              | IndexNLJoin Int

nodeChildren (Scan _) = []
nodeChildren (Index _ _) = []
nodeChildren (Join _ r s) = [r,s]
nodeChildren (Select _ n) = [n]
nodeChildren (Project _ n) = [n]

left  = head . nodeChildren
right = head . tail . nodeChildren 

tableFields (Table tn _ fs) =  [ Field (tn ++ "." ++ fn) fsz | Field fn fsz <- fs ]

tableTupleCount (Table _ cnt _) = cnt

fields (Scan t) = tableFields t
fields (Select _ n) = fields n
fields (Project fs _) = fs
fields (Index t _) = tableFields t
fields (Join _ r s ) = fields r ++ fields s

showJoin (BNLJoin b) = "BNL Join (rb=" ++ show b ++ ")"
showJoin (IndexNLJoin b) = "Index Join (rb=" ++ show b ++ ")"
showJoin (MergeJoin rb sb) = "Merge Join (rb=" ++ show rb ++ ", sb=" ++ show sb ++ ")"
showJoin (HashJoin rb) = "Hash Join (b=" ++ show rb ++ ")"


showTable (Table name _ _) = name
	
showNode (Scan table) = "SCAN " ++ showTable table
showNode (Join typ _ _) = showJoin typ
showNode (Select _ _) = "FILTER"
showNode (Project fs _) = "PROJECT " ++ show fs
showNode (Index t sel) = "INDEX SCAN " ++ showTable t

showNodeSize n = concat [showNode n, " [ ", show (tupleCount n), " @ ", 
                         show (tupleSize n), " bytes = ", 
			 show (pageCount n) ," pages ]\n"]

showNodeCost n = concat [showNode n, " (", show (readCost n), " pages, ",
       show (seekCost n), " seeks)\n"]

showTreeWith f = putStrLn . showTree' 0 where
	showTree' i x = replicate i ' ' ++ f x ++ concatMap
		(showTree' (i+1)) (nodeChildren x)

showTreeCost = showTreeWith showNodeCost
showTreeSize = showTreeWith showNodeSize


tupleCount (Scan (Table _ count _)) = count
tupleCount (Project _ node) = tupleCount node
tupleCount (Select sel node) = tupleCount node `divUp` sel
tupleCount (Join _ r s) = (tupleCount r * tupleCount s)
tupleCount (Index (Table _ count _) s) = count `divUp` s

tableTupleSize (Table _ _ fs) = sum (fieldWidth `map` fs)

tupleSize (Scan t) = tableTupleSize t
tupleSize (Join _ r s) = tupleSize r + tupleSize s
tupleSize (Select _ n) = tupleSize n
tupleSize (Project fs _) = sum (fieldWidth `map` fs)
tupleSize (Index t _) = tableTupleSize t

pageCount n = tupleCount n `divUp` (pageSize `div` tupleSize n)

readCost s@(Scan _) = pageCount s
readCost s@(Index _ _) = pageCount s + indexDepth
readCost (Join typ r s) = case typ of
        BNLJoin br -> readCost r + (pageCount r `divUp`br) * readCost s
        HashJoin b -> 3 * (pageCount r + pageCount s)
        MergeJoin _ _ -> readCost r + readCost s
        IndexNLJoin _ -> readCost r + indexDepth * tupleCount r 
readCost (Select _ node) = readCost node
readCost (Project _ node) = readCost node

seekCost (Scan _) = 1
seekCost (Index _ _) = indexDepth
seekCost (Join typ r s)  = case typ of
        BNLJoin br -> 2 * pageCount r `divUp` br + seekCost r
        HashJoin b -> 2 * (pageCount r + pageCount s + b)
        MergeJoin br bs -> (pageCount r `divUp` br) + (pageCount s `divUp` bs)
        IndexNLJoin br -> (pageCount r `divUp` br) + indexDepth * tupleCount r
seekCost (Select _ node) = seekCost node
seekCost (Project _ node) = seekCost node

leftPKeyJoin  j r s = Select (tupleCount r) $ Join j r s 
rightPKeyJoin j r s = Select (tupleCount s) $ Join j r s 

pkeyLookup t = Index t (tableTupleCount t)

joinToIndex sel (Join (BNLJoin br) r (Scan t)) = [Join (IndexNLJoin br) r (Index t sel)]
joinToIndex _ _ = []

tryJoins :: Node -> [Node]
tryJoins (Join (BNLJoin br) r s) = do
	r' <- tryJoins r
	s' <- tryJoins s
	j <- [BNLJoin br, HashJoin br]
	[Join j r' s']

tryJoins (Project fs n) = fmap (Project fs) (tryJoins n)
tryJoins (Select s n) = fmap (Select s) (tryJoins n)
tryJoins n = [n]

optimize = minimumBy (comparing cost) . tryJoins
