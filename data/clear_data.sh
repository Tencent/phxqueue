clear_store_dir () {
  cd $1/
  rm -f sync
  rm -rf nodedb/
  rm -rf cp/
  ls -lhasp
  cd -
}

clear_lock_dir () {
  cd $1/
  rm -rf nodedb/
  rm -rf mirror/
  ls -lhasp
  cd -
}

clear_store_dir 'store.0'
clear_store_dir 'store.1'
clear_store_dir 'store.2'

clear_lock_dir 'lock.0'
clear_lock_dir 'lock.1'
clear_lock_dir 'lock.2'

