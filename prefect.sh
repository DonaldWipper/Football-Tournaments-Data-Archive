#!/bin/bash
set -e
set +x

VOLUMES_FOLDER=${PWD}/${PREFECT_DIR}/volumes
INITIALIZED_MARKER=${PWD}/${PREFECT_DIR}/volumes/.initialized
DC_ENV_FILE=.env
LOCAL_ENV_FILE=./versions.env


# ------------------------------------


function wait_until_postgres_ready() {
    local iterations=0
    local max_iterations=40
    while [ ${iterations} -le ${max_iterations} ]; do
        set +e
        docker-compose exec postgres bash -c 'pg_isready | grep "accepting connections"' > /dev/null 2>&1
        local ready=$?
        set -e
        if [ ${ready} -eq 0 ]; then
            break
        else
            echo "waiting for postgres to be ready"
            sleep 5
        fi
        (( iterations = iterations + 1 ))
    done
    if [ "${iterations}" -gt ${max_iterations} ]; then
        echo "error starting postgres"
        exit 1
    fi
}


function start_server() {
    docker-compose up -d --force-recreate --no-deps postgres
    wait_until_postgres_ready
    sleep 1
    docker-compose up -d --force-recreate --no-deps prefect-server
    sleep 1
}


function initialize() {
    echo "Environment needs to be initialized...."
    rm -rf ${VOLUMES_FOLDER} && \
        mkdir -p ${VOLUMES_FOLDER}/postgres && \
        mkdir -p ${VOLUMES_FOLDER}/prefect > /dev/null 2>&1
    start_server
    sleep 1
    set +e
    docker-compose exec prefect-server bash -c 'cd ${PWD}/${PREFECT_DIR}/flows && python ./init_orion.py'
    if [ $? -ne 0 ]; then
        echo "ERROR: prefect server failed to initialize"
        exit 1
    fi
    set -e

    touch ${INITIALIZED_MARKER}
}


function start() {
    local server_started=0
    if [ ! -e ${DC_ENV_FILE} ]; then
        ln -s ${LOCAL_ENV_FILE} ${DC_ENV_FILE}
    fi
    if [ ! -d ${VOLUMES_FOLDER} ] || [ ! -f ${INITIALIZED_MARKER} ]; then
        initialize
        server_started=1
    fi
    if [ ${server_started} -eq 0 ]; then
        start_server
        sleep 5
    fi
    docker-compose up -d --force-recreate --no-deps prefect-agent
    status
}


function status() {
    echo ''
    docker-compose ps
    echo ''
}


function stop() {
    echo ''
    docker-compose down
    echo ''
}


function reset() {
    echo 'deleting ALL prefect data'
    rm -rf volumes
    echo 'done!'
}


# ------------------------------------


ROOT_FOLDER=$(dirname $0)
pushd ${ROOT_FOLDER} > /dev/null 2>&1


case "$1" in
    "restart")
        stop
        start
        ;;
    "start")
        start
        ;;
    "status")
        status
        ;;
    "stop")
        stop
        ;;
    "reset")
        stop
        reset
        ;;
    *)
        echo "Unknown option <$1>. Valid options: [ start, stop, restart, status, reset ]"
        exit 1
        ;;
esac

popd > /dev/null 2>&1
