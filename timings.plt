# set datafile separator ","
set terminal png size 1024, 768 font "arial,10.0"
set output 'timings.png'
plot "timings.dat" u 3:4 title 'from-scratch' w l, "timings.dat" u 3:5 title 'DRed' w l, "timings.dat" u 3:6 title 'commit/revert' w l
