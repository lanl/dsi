if [ -z ${SOURCE_BASE_DIRECTORY+x} ]; then
    echo "SOURCE_BASE_DIRECTORY is unset"
    exit 0;
else
    echo "SOURCE_BASE_DIRECTORY is set to '$SOURCE_BASE_DIRECTORY'"; 
fi

source ~/.bash_profile
conda activate cdsi
cd $SOURCE_BASE_DIRECTORY
git checkout -f $CANDIDATE_COMMIT_HASH

make clean;
make COMPILER=GNU;
echo "================================ Compile Done ================================ "l
    
echo "============================= Running CloverLeaf ============================= "j
mpirun -np 2 clover_leaf;