.PHONY: test clean env
.DELETE_ON_ERROR:

# python interpreter to use
export PYTHON=python3
# improve ansible output
export ANSIBLE_CALLBACK_RESULT_FORMAT=yaml
# reasonable default verbosity, crank to debug issues
export ANSIBLE_VERBOSITY=0
# pipeline operations over ssh
export ANSIBLE_PIPELINING=true
# show breakdown of task times
export ANSIBLE_CALLBACKS_ENABLED=profile_tasks
# not useful since they are throwaways
export ANSIBLE_HOST_KEY_CHECKING=false
# run all machines in parallel
export ANSIBLE_FORKS=10

ifeq ($(AWS_ACCESS_KEY_ID),)
$(error AWS_ACCESS_KEY_ID is not set)
endif

ifeq ($(AWS_SECRET_ACCESS_KEY),)
$(error AWS_SECRET_ACCESS_KEY is not set)
endif

ifeq ($(PATCH_BRANCH),)
$(error PATCH_BRANCH is not set)
endif

# runs benchmark across AWS instances
# always destroys anything created out there
test: env
	bash -c "set -ex; trap 'build/.env/bin/ansible-playbook destroy.yml' EXIT; build/.env/bin/ansible-playbook playbook.yml"

clean:
	rm -rf build

# keep venv in sync with requirements.txt
env: build/.env/bin/activate
build/.env/bin/activate: requirements.txt
	rm -rf build/.env
	$(PYTHON) -m venv build/.env
	build/.env/bin/pip install --no-input -U -r requirements.txt
	touch build/.env/bin/activate
