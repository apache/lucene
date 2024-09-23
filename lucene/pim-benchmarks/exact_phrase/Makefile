INDEX_DIRECTORY:=/scratch/nney/lucene_data
DATASET_PATH:=./datasets/wiki_subset
#DATASET_PATH:=/home/shared/index_search/wikipedia/files
#OUTPUT_PREFIX:=full_wiki
#OUTPUT_PREFIX:=superset
OUTPUT_PREFIX:=subset
QUERIES:=./queries/requests.txt
#QUERIES:=./queries/requests_one_phrase.txt
NB_DPUS=1024
NB_THREADS ?= 64
NB_TOPDOCS ?= 100
PIM_LUCENE_DIR=/home/upmemstaff/nney/lucene/pim-lucene
JNI_LIB=${PIM_LUCENE_DIR}/lucene/sandbox/dpu/build
PROFILING_OPTIONS=-XX:+FlightRecorder -XX:StartFlightRecording=duration=200s,filename=flight.jfr
OUTPUT_DIR ?= outputs
OUTPUT_FILE_PREFIX ?= ${OUTPUT_PREFIX}
 
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
	@echo 
	@echo 'If you need to run in debug mode on DPU, prefix your make run command with'
	@echo 'ASAN_OPTIONS=detect_leaks=0 LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.5'
	@echo '-------------------------------------------------------------------------------'

index: ${DATASET_PATH}
	source shell/setclasspath.sh && java src/IndexRAM.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}"

index_dpu: ${DATASET_PATH}
	source shell/setclasspathDPU.sh && java src/IndexRAMDPU.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -nbdpus ${NB_DPUS}

index_dpu_refresh: ${DATASET_PATH}
	source shell/setclasspathDPU.sh && java src/IndexRAMDPU.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -nbdpus ${NB_DPUS} -dpuonly

run:
	source shell/setclasspath.sh && javac src/SearchWiki.java && java SearchWiki -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee ${OUTPUT_DIR}/out_${OUTPUT_FILE_PREFIX}

run_parallel:
	source shell/setclasspath.sh && javac src/SearchWikiMultiThread.java && java ${PROFILING_OPTIONS} SearchWikiMultiThread -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} -nthreads ${NB_THREADS} -ntopdocs ${NB_TOPDOCS} |&tee ${OUTPUT_DIR}/out_parallel_${OUTPUT_FILE_PREFIX}

run_parallel_old:
	source shell/setclasspath_old.sh && javac src/SearchWikiMultiThread.java && java ${PROFILING_OPTIONS} SearchWikiMultiThread -index /home/shared/index_search/lucene/index -queries ${QUERIES} |&tee ${OUTPUT_DIR}/out_parallel_${OUTPUT_FILE_PREFIX}


run_prefix_parallel:
	source shell/setclasspath.sh && javac src/SearchWikiPrefixMultiThread.java && java ${PROFILING_OPTIONS} SearchWikiPrefixMultiThread -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee ${OUTPUT_DIR}/out_prefix_parallel_${OUTPUT_FILE_PREFIX}

run_fuzzy_parallel:
	source shell/setclasspath.sh && javac src/SearchWikiFuzzyMultiThread.java && java ${PROFILING_OPTIONS} SearchWikiFuzzyMultiThread -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee ${OUTPUT_DIR}/out_fuzzy_parallel_${OUTPUT_FILE_PREFIX}

run_dpu:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPU.java && java ${PROFILING_OPTIONS} -Djava.library.path=${JNI_LIB} -Dlucene.pim.dir=${PIM_LUCENE_DIR} SearchWikiDPU -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee ${OUTPUT_DIR}/out_dpu_${OUTPUT_FILE_PREFIX}

run_dpu_parallel:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPUMultiThread.java && java ${PROFILING_OPTIONS} -Djava.library.path=${JNI_LIB} -Dlucene.pim.dir=${PIM_LUCENE_DIR} SearchWikiDPUMultiThread -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} -nthreads ${NB_THREADS} -ntopdocs ${NB_TOPDOCS} |&tee ${OUTPUT_DIR}/out_dpu_parallel_${OUTPUT_FILE_PREFIX}

${DATASET_PATH}:
	echo "Decompressing dataset ${DATASET_PATH}.tgz"
	tar -xzf ${DATASET_PATH}.tgz -C datasets

