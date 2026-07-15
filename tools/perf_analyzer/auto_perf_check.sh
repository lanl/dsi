#! /bin/bash

# make sure that cdsi environment is activated
if [[ $CONDA_DEFAULT_ENV != 'cdsi' ]]; then
    echo "activate conda cdsi environment."
    exit 0
fi
if [ -z ${SOURCE_BASE_DIRECTORY+x} ]; then
    echo "SOURCE_BASE_DIRECTORY is unset"
    exit 0;
else
    echo "SOURCE_BASE_DIRECTORY is set to '$SOURCE_BASE_DIRECTORY'"; 
fi

# SOURCE_BASE_DIRECTORY="/Users/ssakin/projects/CloverLeaf/CloverLeaf_ref"
MPI_THREADS=4
export CHECK_PREV_COMMITS=15
export OMP_NUM_THREADS=4
base_directory=$(pwd)

run_and_check_commit() {
    echo "current commit hash $1"

    cd $SOURCE_BASE_DIRECTORY
    git checkout $1
    make clean
    make COMPILER=GNU
    echo "================================ Compile Done ================================ "
    
    echo "============================= Running CloverLeaf ============================= "
    mpirun -np $MPI_THREADS clover_leaf
    cp clover.out $base_directory"/clover_output/clover_$1.out"
    echo "======================= CloverLeaf Executed for has $1 ======================= "
    
    echo "=========================== Running output parser ============================ "
    cd $base_directory
    python3 parse_clover_output.py --testname random_test --gitdir $SOURCE_BASE_DIRECTORY
    echo "============================ Output CSV updated ============================== "
}

track_variables() {
    echo "current commit hash $1"

    cd $SOURCE_BASE_DIRECTORY
    git checkout $1
    
    echo "=========================== Running code sensing ============================ "
    cd $base_directory
    python3 code_sensing.py --testname random_test --gitdir $SOURCE_BASE_DIRECTORY
    echo "============================ Output CSV updated ============================== "
}

cd $SOURCE_BASE_DIRECTORY
prev_hash=( $(git log master -n "$CHECK_PREV_COMMITS" --format=format:%h) )

for c_hash in "${prev_hash[@]}"
do
#    run_and_check_commit $c_hash
    track_variables $c_hash
done

cd $SOURCE_BASE_DIRECTORY
git checkout master
echo "=========================== Auto Perf Script Completed ============================ "
