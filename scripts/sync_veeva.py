import os
import shutil
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
SOURCE_ROOT = r"c:/Users/karta/Downloads/Projects/veeva-direct-data-accelerator"
DEST_ROOT = os.path.join(os.getcwd(), "veeva_accelerator")

DIRS_TO_SYNC = [
    {
        "source": os.path.join(SOURCE_ROOT, "accelerators", "redshift"),
        "dest": os.path.join(DEST_ROOT, "accelerators", "redshift")
    },
    {
        "source": os.path.join(SOURCE_ROOT, "common"),
        "dest": os.path.join(DEST_ROOT, "common")
    }
]

def sync_directories():
    if not os.path.exists(SOURCE_ROOT):
        logging.error(f"Source directory not found: {SOURCE_ROOT}")
        return

    # Create empty __init__.py at DEST_ROOT to make it a package
    if not os.path.exists(DEST_ROOT):
        os.makedirs(DEST_ROOT)
        with open(os.path.join(DEST_ROOT, '__init__.py'), 'w') as f:
            pass

    for item in DIRS_TO_SYNC:
        source_dir = item["source"]
        dest_dir = item["dest"]

        logging.info(f"Syncing from {source_dir} to {dest_dir}")

        if os.path.exists(dest_dir):
            logging.info(f"Removing existing directory: {dest_dir}")
            shutil.rmtree(dest_dir)

        try:
            shutil.copytree(source_dir, dest_dir)
            logging.info(f"Successfully copied {source_dir} to {dest_dir}")
            
            # Create __init__.py files in subdirectories to ensure they are treated as packages
            # This is recursive for the newly created directories
            for root, dirs, files in os.walk(dest_dir):
                init_file = os.path.join(root, "__init__.py")
                if not os.path.exists(init_file):
                    with open(init_file, "w") as f:
                        pass
        except Exception as e:
            logging.error(f"Failed to copy {source_dir} to {dest_dir}: {e}")

if __name__ == "__main__":
    logging.info("Starting Veeva Accelerator Sync...")
    sync_directories()
    logging.info("Sync complete.")
