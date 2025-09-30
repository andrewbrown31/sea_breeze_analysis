#!/bin/bash

for i in $(seq 1979 1 2024); do

    let j=i+1

     cp /home/548/ab4502/working/sea_breeze_analysis/barra_c/filter_F.sh /home/548/ab4502/working/sea_breeze_analysis/barra_c/filter_jobs/filter_F_$i.sh

      sed -i "s/YYYY1/$i/g" /home/548/ab4502/working/sea_breeze_analysis/barra_c/filter_jobs/filter_F_$i.sh
      sed -i "s/YYYY2/$j/g" /home/548/ab4502/working/sea_breeze_analysis/barra_c/filter_jobs/filter_F_$i.sh

      qsub /home/548/ab4502/working/sea_breeze_analysis/barra_c/filter_jobs/filter_F_$i.sh

done