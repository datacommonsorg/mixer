# Submits a cloud build job and pipes output to a file
# Moves the output file to folder `success` if return code is 0
# Otherwise, moves it to folder `failed`
#
# Parameters
# $1 is the path to the config file
# $2 is the path to the code folder
# $3 is the name of the file to pipe output to
function submit_cloud_build {
	gcloud builds submit --config $1 $2 &> $3
	if [ $? -ne 0 ]; then
		mv $3 /workspace/failed/
	else
		mv $3 /workspace/success/
	fi
}


function build_import {
	git clone https://github.com/datacommonsorg/import
	submit_cloud_build import/build/cloudbuild.java.yaml import import_java.out.txt
	submit_cloud_build import/build/cloudbuild.npm.yaml import import_npm.out.txt
}

function build_data {
	git clone https://github.com/datacommonsorg/data
	submit_cloud_build data/cloudbuild.go.yaml data data_go.out.txt
	submit_cloud_build data/cloudbuild.py.yaml data data_py.out.txt
}

function build_api_r {
	git clone https://github.com/datacommonsorg/api-r
	submit_cloud_build api-r/cloudbuild.yaml api-r api-r.out.txt
}

function build_api_python {
	git clone https://github.com/datacommonsorg/api-python
	submit_cloud_build api-python/cloudbuild.yaml api-python api-python.out.txt
}

function build_mixer {
	git clone https://github.com/datacommonsorg/mixer
	submit_cloud_build mixer/build/ci/cloudbuild.test.yaml mixer mixer.out.txt
}

function build_recon {
	git clone https://github.com/datacommonsorg/reconciliation
	submit_cloud_build reconciliation/build/ci/cloudbuild.test.yaml reconciliation reconciliation.out.txt
}

function build_website {
	git clone https://github.com/datacommonsorg/website
	submit_cloud_build website/build/ci/cloudbuild.npm.yaml website website.out.txt
	submit_cloud_build website/build/ci/cloudbuild.py.yaml website website.out.txt
	submit_cloud_build website/build/ci/cloudbuild.webdriver.yaml website website.out.txt
}

mkdir allrepos_tmp
cd allrepos_tmp

mkdir /workspace/failed
mkdir /workspace/success


# Parallelize the build functions, reference: https://stackoverflow.com/a/26240420
pids=""
build_cmds_to_run=('build_import' 'build_data' 'build_api_r' 'build_api_python' 'build_mixer' 'build_recon' 'build_website')
for build_cmd in ${build_cmds_to_run[*]}; do
	echo "running build function: $build_cmd"
	$build_cmd &
	pid=$!
	echo "$build_cmd pid is $pid"
	pids="$pids $pid"
done

echo "waiting on pids: ${pids[*]}"
wait $pids
echo "all processes returned, returning."
