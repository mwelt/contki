# set datafile separator ","
# set terminal png size 1024, 768 font "arial,10.0"
set terminal postscript portrait enhanced color dashed lw 1 "Arial" 12
set output 'timings.ps'
set xlabel 'no. of entities'
set ylabel 't in ms'
# set ytics 50
# set y2label 'no. of coinflips'
plot "timings.dat" u 1:2 title 'from-scratch' w l, "timings.dat" u 1:3 title 'DRed' w l, "timings.dat" u 1:4 title 'commit/revert' w l

# ,  "timings.dat" u 1:5 title 'initial coinflips' w l axis x1y2
