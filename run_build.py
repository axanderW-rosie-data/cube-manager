import sys 
from cube_manager_package.cube_manager_updated import process_builds

# run the build process
if __name__ == '__main__':
    config_file = sys.argv[1]
    process_builds(config_file)

sys.exit()
