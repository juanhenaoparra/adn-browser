1. To run the zinsearch database: `docker build . -t adn-browser-zs`
2. To execute the image: `docker run --user $(id -u docker):$(id -g docker) -v ./data:/data -e ZINC_DATA_PATH="/data" -p 4080:4080 --name zincsearch adn-browser-zs`
3. To connect to the VM: `ssh docker@<ip>`