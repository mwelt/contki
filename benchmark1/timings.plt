# set datafile separator ","
# set terminal png size 1024, 768 font "arial,10.0"
set terminal postscript portrait enhanced color dashed lw 1 "Arial" 12
set output 'timings.ps'
set xlabel 'no. of edges'
set ylabel 't in ms'
plot "timings.dat" u 3:4 title 'from-scratch' w l, "timings.dat" u 3:5 title 'DRed' w l, "timings.dat" u 3:6 title 'commit/revert' w l
