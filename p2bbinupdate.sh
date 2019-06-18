mkdir p2bbin&&cd p2bbin
git init
git remote add origin https://github.com/warnov/AzureStorageUtilities.Binaries.Linux.git
git pull origin master
chmod +x AzureStorageUtilities.PageToBlockMover.BatchWorker
chmod +x azcopy
PATH=$PATH:/p2bbin
