#!/bin/sh
$HBOX_HOME/bin/hbox-submit \
   --app-type "tensorflow" \
   --app-name "tf-estimator-demo" \
   --files demo.py \
   --worker-memory 2G \
   --worker-num 3 \
   --worker-cores 2 \
   --ps-memory 2G \
   --ps-num 1 \
   --ps-cores 2 \
   --tf-evaluator true \
   --queue default \
   python demo.py --data_path=hdfs://hbox.test.host1:9000/tmp/data/tfEstimator --model_path=hdfs://hbox.test.host1:9000/tmp/estimatorDemoModel
