if [ -z ${SOURCE_BASE_DIRECTORY+x} ]; then
    echo "SOURCE_BASE_DIRECTORY is unset"
    exit 0;
else
    echo "SOURCE_BASE_DIRECTORY is set to '$SOURCE_BASE_DIRECTORY'"; 
fi

source ~/.bash_profile
conda activate cdsi

export TAU_PROFILE=1
export PROFILEDIR=$SOURCE_BASE_DIRECTORY


cd $SOURCE_BASE_DIRECTORY
git checkout -f $CANDIDATE_COMMIT_HASH

cp ../clover.in .

make clean;
make COMPILER=GNU;
echo "================================ Compile Done ================================ "l
    
echo "============================= Running CloverLeaf ============================= "j
mpirun -np 2 tau_exec $SOURCE_BASE_DIRECTORY/clover_leaf
pprof -s > tau_results
cd -
conda deactivate