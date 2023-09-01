# matrix-multiplication-spark
 Matrix Multiplication Using Spark MapReduce
 
implementing a single mapreduce on PySpark to compute a matrix multiplication, i.e., Cnxn = Anxn X Bnxn.

Following is the pseudocode:
 
The Map Function: For each element aij of matrix A, produce all the key-value pairs ((i, k), (M, j, aij)) for k = 0, 1, 2, …, n-1. Similarly, for each element bjk of B, produce all the key-value pairs ((i, k), (N, j, bjk)) for i = 0, 1, 2, …, n-1. M and N are really bits to tell which of the two matrices a value comes from, e.g., you can make M = 0 and N = 1 to indicate the matrix on the right or not.
 
The Reduce Function: For each key (i, k), sum up all Aij, * Bjk from (M, j, Aij,) and (N, j, Bjk) for j = 0, 1, 2, …, n-1, and output key-value pair ((i, k), Cik). Each key (i, k) will have an associated list with all the values (M, j, aij) and (N, j, bjk), for all possible values of j. The Reduce function needs to connect the two values on the list that have the same value of j, for each j. An easy way to do this step is to sort by j the values that begin with M and sort by j the values that begin with N, in separate lists. The jth values on each list must have their third components, aij and bjk extracted and multiplied. Then, these products are summed and the result is paired with ((i, k), cik) in the output of the Reduce function.
 
To simplify, consider the smallest 2x2 matrix multiplication problem like [[a00, a01], [a10, a11]] * [[b00, b01], [b10, b11]] = [[a00b00+a01b10, a00b01+a01b11], [a10b00+a11b10, a10b01+a11b11]] = [[c00, c01], [c10, c11]]. So, a00 should send to c00 and c01, and same for others. Similarly, b00 should send to c00 and c10, and same as others.
 
For example, if A = [[1, 2], [7, 8]] and B = [[3, 4], [5, 6]]. The mapper will generate key-value pairs as ((0, 0), (0, 0, 1)), ((0, 1), (0, 0, 1)), ((0, 0), (0, 1, 2)), ((0, 1), (0, 1, 2)), ((1, 0), (0, 0, 7)), ((1, 1), (0, 0, 7)), ((1, 0), (0, 1, 8)), ((1, 1), (0, 1, 8)), ((0, 0), (1, 0, 3)), ((1, 0), (1, 0, 3)), ((0, 1), (1, 0, 4)), ((1, 1), (1, 0, 4)), ((0, 0), (1, 1, 5)), ((1, 0), (1, 1, 5)), ((0, 1), (1, 1, 6)) , ((1, 1), (1, 1, 6)). The system sorts the keys and assigns the same key to one reducer, so the reducers are:
	1.	reducer for key (0, 0) gets [(0, 0, 1), (0, 1, 2), (1, 0, 3), (1, 1, 5)], and emit((0, 0), (1*3 + 2*5)).
	2.	reducer for key (0, 1) gets [(0, 0, 1), (0, 1, 2), (1, 0, 4), (1, 1, 6)], and emit((0, 1), (1*4 + 2*6)).
	3.	reducer for key (1, 0) gets [(0, 0, 7), (0, 1, 8), (1, 0, 3), (1, 1, 5)], and emit((1, 0), (7*3 + 8*5)).
	4.	reducer for key (1, 1) gets [(0, 0, 7), (0, 1, 8), (1, 0, 4), (1, 1, 6)], and emit((1, 1), (7*4 + 8*6)).
 
The input are two files, A.text and B.text, and both have fixed number of real numbers per line.
