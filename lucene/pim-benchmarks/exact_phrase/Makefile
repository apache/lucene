INDEX_DIRECTORY:=/tmp/lucene_data
DATASET_PATH:=./datasets/wiki_subset
OUTPUT_PREFIX:=subset
QUERIES:=./queries/requests_subset.txt
#QUERIES:=./queries/requests_one_phrase.txt
NB_DPUS=16
JNI_LIB=/home/jlegriel/Work/lucene-PIM/lucene-upmem-fork/pim-lucene/lucene/sandbox/dpu
 
SHELL := /bin/bash

.PHONY: help index run index_dpu run_dpu

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

index:
	source shell/setclasspath.sh && java src/IndexRAM.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}"

index_dpu:
	source shell/setclasspathDPU.sh && java src/IndexRAMDPU.java -dataset ${DATASET_PATH} -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -nbdpus ${NB_DPUS}

run:
	source shell/setclasspath.sh && javac src/SearchWiki.java && java SearchWiki -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_${OUTPUT_PREFIX}

run_parallel:
	source shell/setclasspath.sh && javac src/SearchWikiMultiThread.java && java SearchWikiMultiThread -index "${INDEX_DIRECTORY}/index_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_parallel_${OUTPUT_PREFIX}

run_dpu:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPU.java && java -Djava.library.path=${JNI_LIB} SearchWikiDPU -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_dpu_${OUTPUT_PREFIX}

run_dpu_parallel:
	source shell/setclasspathDPU.sh && javac src/SearchWikiDPUMultiThread.java && java -Djava.library.path=${JNI_LIB} SearchWikiDPUMultiThread -index "${INDEX_DIRECTORY}/index_dpu_${OUTPUT_PREFIX}" -queries ${QUERIES} |&tee outputs/out_dpu_parallel_${OUTPUT_PREFIX}

