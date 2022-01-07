You will need CMake and Boost to build the libraries. This steps also apply when installing with Pip, as the distribution is a source code bundle.


Four Ubuntu:
``` 
sudo apt-get install -y python3-dev python3-pip
sudo apt-get install -y cmake libboost-all-dev libboost-log-dev libboost-log-dev libboost-program-options-dev libboost-regex-dev 
sudo apt-get install -y  zlib1g-dev libyaml-cpp-dev git

```


or built locally downloading and building
```
https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson
https://github.com/jbeder/yaml-cpp/
```
