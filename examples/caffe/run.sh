#!/bin/sh
$HBOX_HOME/bin/hbox-submit \
   --app-type "caffe" \
   --app-name "caffe_demo" \
   --input /tmp/data/caffe/mnist_train_lmdb#mnist_train_lmdb \
   --input /tmp/data/caffe/mnist_test_lmdb#mnist_test_lmdb \
   --output /tmp/caffe_output#model \
   --files lenet_train_test.prototxt,lenet_solver.prototxt \
   --cacheArchive /tmp/data/caffe/caffe.zip#caffe \
   --worker-memory 10G \
   --worker-cores 2 \
   --queue default \
   ./caffe/build/tools/caffe train --solver=./lenet_solver.prototxt
