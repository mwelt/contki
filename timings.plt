set datafile separator ","
set terminal png size 1024, 768 font "arial,10.0"
set output 'timings.png'
plot "timings.dat" u 3:4 title 'naive' w l, "timings.dat" u 3:5 title 'seminaive' w l, "timings.dat" u 3:6 title 'seminaiveext' w l
