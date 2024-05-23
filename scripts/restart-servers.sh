if [ "$#" -ne 1 ]; then
    echo "Usage: $0 [AGE_ROOT]"
    exit -1
fi

export AGE_ROOT=$1
python3 ${AGE_ROOT}/scripts/AGE_admin.py -a stop
sleep 1
python3 ${AGE_ROOT}/scripts/AGE_admin.py -a start -i true
