INDEX_DIRECTORY:=/tmp/lucene_data
DATASET_PATH:=./datasets/wiki_subset
OUTPUT_PREFIX:=subset
QUERIES:=./queries/requests_subset.txt
#QUERIES:=./queries/requests_one_phrase.txt
NB_DPUS=64
PIM_LUCENE_DIR=/home/upmemstaff/jlegriel/lucene/pim-lucene
JNI_LIB=${PIM_LUCENE_DIR}/lucene/sandbox/dpu
#PROFILING_OPTIONS=-XX:+FlightRecorder -XX:StartFlightRecording=duration=200s,filename=flight.jfr
 
SHELL := /bin/bash

.PHONY: help index run run_parallel index_dpu run_dpu run_dpu_parallel

help:
	@echo '-------------------------------------------------------------------------------'
	@echo 'PIM Lucene Search on Wikipedia. Targets: '
	@echo '-------------------------------------------------------------------------------'
	@echo 'index:	create lucene index for files in the dataset'
	@echo 'run:	  run lucene search with phrase queries'
	@echo 'index_dpu:	create lucene index for files in the dataset for DPUs'
	@echo 'run_dpu:	run lucene search with phrase queries using DPUs'
	@echo 'run_dpu_parallel: same as run_dpu but using multiple searcher threads'
	@echo '-------------------------------------------------------------------------------'

index: ${DATASET_PATH}
	source shell/setclasspath.sh && java src/IndexRAM.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}"

index_dpu: ${DATASET_PATH}
	source shell/setclasspathDPU.sh && java src/IndexRAMDPU.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -nbdpus ${NB_DPUS}

run:
	source shell/setclasspath.sh && javac src/SearchWiki.java && java SearchWiki -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_${OUTPUT_PREFIX}

run_parallel:
	source shell/setclasspath.sh && javac src/SearchWikiMultiThread.java && java SearchWikiMultiThread -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_parallel_${OUTPUT_PREFIX}

run_dpu:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPU.java && java ${PROFILING_OPTIONS} -Djava.library.path=${JNI_LIB} -Dlucene.pim.dir=${PIM_LUCENE_DIR} SearchWikiDPU -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_dpu_${OUTPUT_PREFIX}

run_dpu_parallel:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPUMultiThread.java && java ${PROFILING_OPTIONS} -Djava.library.path=${JNI_LIB} -Dlucene.pim.dir=${PIM_LUCENE_DIR} SearchWikiDPUMultiThread -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_dpu_parallel_${OUTPUT_PREFIX}

${DATASET_PATH}:
	echo "Decompressing dataset ${DATASET_PATH}.tgz"
	tar -xzf ${DATASET_PATH}.tgz -C datasets

