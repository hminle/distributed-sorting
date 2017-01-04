rm -rf /data4/490write/output
OUT='/data4/490write/output'
mkdir $OUT
sync && echo 3 | sudo tee /proc/sys/vm/drop_caches
start=`date +"%s"`

#Run Slave
sbt "run-main core.Slave $1 /data1/490read/ascii/100m/input0.data $OUT"

end=`date +"%s"`
secs=$((end-start))
echo "Total Time:"
printf '%02dh:%02dm:%02ds\n' $(($secs/3600)) $(($secs%3600/60)) $(($secs%60))
stty erase ^H
# Print total output size
echo "Total Output Size"
ls -FGl $OUT | awk '{ total += $4; print }; END { print total }'
