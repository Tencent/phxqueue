clear_dir () {
  cd $1/
  rm -f *.INFO
  rm -f *.WARNING
  rm -f *.ERROR
  rm -f *.INFO.*
  rm -f *.WARNING.*
  rm -f *.ERROR.*
  ls -lhasp
  cd -
}

clear_dir 'store.0'
clear_dir 'store.1'
clear_dir 'store.2'

clear_dir 'consumer.0'
clear_dir 'consumer.1'
clear_dir 'consumer.2'

clear_dir 'lock.0'
clear_dir 'lock.1'
clear_dir 'lock.2'

clear_dir 'scheduler.0'
clear_dir 'scheduler.1'
clear_dir 'scheduler.2'

clear_dir 'mqttbroker.0'

