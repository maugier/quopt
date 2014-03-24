import Control.Monad.Reader

tSeek = 0.01
tRead = 0.00
indexDepth = 4
pageSize = 1024

divUp q d = 0 - (q `div` (-d))

data Field = Field { fieldName :: String
                   , fieldWidth :: Int }

data Node = Table String Int [Field]
          | Join JoinType Node Node Int
          | Select Int Node
          | Project [Field] Node

data JoinType = BNLJoin Int
              | HashJoin Int
              | MergeJoin Int Int
              | IndexNLJoin Int

fields (Table tn _ fs) = [ Field (tn ++ "." ++ fn) fsz | Field fn fsz <- fs ]
fields (Project fs _) = fs

showJoin (BNLJoin b) = "BNL Join (rb=" ++ show b ++ ")"
showJoin (IndexNLJoin b) = "Index Join (rb=" ++ show b ++ ")"
showJoin (MergeJoin rb sb) = "Merge Join (rb=" ++ show rb ++ ", sb=" ++ show sb ++ ")"
showJoin (HashJoin rb) = "Hash Join (b=" ++ show rb ++ ")"

showTree = putStrLn . showTree' 0 where
        showTree' i x = concat (replicate i "  ") ++ showTree'' x where
                showTree'' (Table name count size) = "TABLE " ++ name 
                     ++ " (" ++ show count ++ " tuples @ " ++ show size ++ ")"
                showTree'' (Join typ r s _) = showJoin typ ++ "\n" 
                   ++ showTree' (i+1) r ++ "\n"
                   ++ showTree' (i+1) s 
                showTree'' (Select _ node) = "FILTER\n" ++ showTree' (i+1) node
                showTree'' (Project _ node) = "PROJECT\n" ++ showTree' (i+1) node




tupleCount (Table _ count _) = count
tupleCount (Project _ node) = tupleCount node
tupleCount (Select sel node) = tupleCount node `divUp` sel
tupleCount (Join _ r s sel) = (tupleCount r * tupleCount s) `divUp` sel

tupleSize (Table _ _ fields) = sum (fieldWidth `map` fields)
tupleSize (Join _ r s _) = tupleSize r + tupleSize s
tupleSize (Select _ n) = tupleSize n
tupleSize (Project size _) = size

pageCount n = tupleCount n `divUp` (pageSize `div` tupleSize n)

readCost t@(Table _ _ _) = pageCount t
readCost (Join typ r s _) = case typ of
        BNLJoin br -> pageCount r + (pageCount r `divUp`br) * pageCount s
        HashJoin b -> 3 * (pageCount r + pageCount s)
        MergeJoin _ _ -> pageCount r + pageCount s
        IndexNLJoin _ -> pageCount r + indexDepth * tupleCount r 
readCost (Select _ node) = readCost node
readCost (Project _ node) = readCost node

seekCost t@(Table _ _ _) = 1
seekCost (Join typ r s _) = case typ of
        BNLJoin br -> 2 * pageCount r `divUp` br
        HashJoin b -> 2 * (pageCount r + pageCount s + b)
        MergeJoin br bs -> (pageCount r `divUp` br) + (pageCount s `divUp` bs)
        IndexNLJoin br -> (pageCount r `divUp` br) + indexDepth * tupleCount r
seekCost (Select _ node) = seekCost node
seekCost (Project _ node) = seekCost node

students = Table "Students" 16000 [Field "name" 60, Field "sid" 4]
taken = Table "Taken" 256000 [Field "sid" 4, Field "cid" 4]
courses = Table "Courses" 1600 [Field "cname" 60, Field "cid" 4]

st = Join (BNLJoin 10) students taken (tupleCount students)

pi_st = Project 64 st

final = Join (BNLJoin 10) pi_st courses (tupleCount pi_st * 10)
