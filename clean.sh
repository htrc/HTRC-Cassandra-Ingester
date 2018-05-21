#!/bin/bash

DATE=`date '+%Y-%m-%d_%H:%M:%S'`

echo $DATE

prefix="run"

dir=$prefix"_"$DATE

echo $dir

mkdir $dir

mv *.txt $dir
mv logs $dir
cp -r conf $dir

if [ -d archive ]; then
    mv archive $dir/
fi
