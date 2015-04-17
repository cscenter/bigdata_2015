Из Mining Massive Datasets известно, что:
> The *threshold*, that is, the value of similarity s at which the rise becomes steepest, is a function of b and r. An approximation to the threshold is (1/b)^(1/r)

Следовательно, стоит подбирать r и b такими, чтобы выполнялось:

    (1/b)^{(1/r)} = 1/2 
    b*r = n

Вычислим формулы для r и b:

    1/b = (1/2)^r 
    b = 2^r 
    2^r = n/r
    n = r e^(r log 2)
    n log 2 = r log 2 e^(r log 2)
    r log 2 = W(n log 2)  // Lambert W-function 
    r = W(n log 2) / log 2}

А поскольку асимптотика W(x) при x -> ∞ есть log x - log log x, получаем, что для достаточно больших n:

    r ~ (log(n log 2) - log(log(n log 2)) / log 2
    
К примеру, для n = 20:

    r = 3
    b = 7
    threshold = 0.550321208149
   
Для n = 1e6:

    r = 26
    b = 38461539
    threshold = 0.510820367819
    
Параметр r вычисляется программно и подаётся в первый mapreduce. 

Параметр k: в книге говорится только то, что он должен быть достаточно большим. 
Пока не придумал ничего лучше, кроме как взять k = 2^32, то есть, по сути, отказаться от бакетов.