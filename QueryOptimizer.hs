import Control.Monad.Reader

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

data Table = Table String Int [Field]

data Node = Scan Table
          | Join JoinType Node Node Int
          | Select Int Node
          | Project [Field] Node
	  | Index Table Int

data JoinType = BNLJoin Int
              | HashJoin Int
              | MergeJoin Int Int
              | IndexNLJoin Int

nodeChildren (Scan _) = []
nodeChildren (Index _ _) = []
nodeChildren (Join _ r s _) = [r,s]
nodeChildren (Select _ n) = [n]
nodeChildren (Project _ n) = [n]

tableFields (Table tn _ fs) =  [ Field (tn ++ "." ++ fn) fsz | Field fn fsz <- fs ]

fields (Scan t) = tableFields t
fields (Select _ n) = fields n
fields (Project fs _) = fs
fields (Index t _) = tableFields t
fields (Join _ r s _) = fields r ++ fields s

showJoin (BNLJoin b) = "BNL Join (rb=" ++ show b ++ ")"
showJoin (IndexNLJoin b) = "Index Join (rb=" ++ show b ++ ")"
showJoin (MergeJoin rb sb) = "Merge Join (rb=" ++ show rb ++ ", sb=" ++ show sb ++ ")"
showJoin (HashJoin rb) = "Hash Join (b=" ++ show rb ++ ")"


showTable (Table name count fields) = concat [name,
	" (",show count, " tuples @ ", show (sum $ fieldWidth `map` fields), ")"]
	
showNode (Scan table) = "SCAN " ++ showTable table
showNode (Join typ _ _ _) = showJoin typ
showNode (Select _ _) = "FILTER"
showNode (Project _ _) = "PROJECT"
showNode (Index t sel) = "INDEX SCAN " ++ showTable t

showNodeCost n = concat [showNode n, " [", show (readCost n), " reads, ",
       show (seekCost n), " seeks]\n"]

showTree = putStrLn . showTree' 0 where
        showTree' i x = concat (replicate i "  ") ++ showNodeCost x
	   ++ concatMap (showTree' (i+1)) (nodeChildren x)



tupleCount (Scan (Table _ count _)) = count
tupleCount (Project _ node) = tupleCount node
tupleCount (Select sel node) = tupleCount node `divUp` sel
tupleCount (Join _ r s sel) = (tupleCount r * tupleCount s) `divUp` sel
tupleCount (Index (Table _ count _) s) = count `divUp` s

tableTupleSize (Table _ _ fs) = sum (fieldWidth `map` fs)

tupleSize (Scan t) = tableTupleSize t
tupleSize (Join _ r s _) = tupleSize r + tupleSize s
tupleSize (Select _ n) = tupleSize n
tupleSize (Project fs _) = sum (fieldWidth `map` fs)
tupleSize (Index t _) = tableTupleSize t

pageCount n = tupleCount n `divUp` (pageSize `div` tupleSize n)

readCost s@(Scan _) = pageCount s
readCost s@(Index _ _) = pageCount s + indexDepth
readCost (Join typ r s _) = case typ of
        BNLJoin br -> readCost r + (pageCount r `divUp`br) * readCost s
        HashJoin b -> 3 * (pageCount r + pageCount s)
        MergeJoin _ _ -> readCost r + readCost s
        IndexNLJoin _ -> readCost r + indexDepth * tupleCount r 
readCost (Select _ node) = readCost node
readCost (Project _ node) = readCost node

seekCost (Scan _) = 1
seekCost (Index _ _) = indexDepth
seekCost (Join typ r s _) = case typ of
        BNLJoin br -> 2 * pageCount r `divUp` br
        HashJoin b -> 2 * (pageCount r + pageCount s + b)
        MergeJoin br bs -> (pageCount r `divUp` br) + (pageCount s `divUp` bs)
        IndexNLJoin br -> (pageCount r `divUp` br) + indexDepth * tupleCount r
seekCost (Select _ node) = seekCost node
seekCost (Project _ node) = seekCost node

-- Test code

name = Field "name" 60
sid = Field "sid" 4
cid = Field "cid" 4
cname = Field "cname" 60

students = Table "Students" 16000 [name,sid]
taken = Table "Taken" 256000 [sid,cid]
courses = Table "Courses" 1600 [cname,cid]

st = Join (BNLJoin 10) (Scan students) (Scan taken) (tupleCount (Scan students))

pi_st = Project [name,cid] st

final1 = Join (BNLJoin 10) pi_st (Scan courses) (tupleCount pi_st * 10)
final2 = Join (BNLJoin 10)
		(Project [sid] 
		  (Join (BNLJoin 10) 
		     (Project [cid] (Select 1600 (Scan courses)))
                     (Scan taken)
		     (256000 `div` 160)))
		(Scan students) 16000
