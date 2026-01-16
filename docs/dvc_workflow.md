# DVC Workflow for Dataset Updates (Task 8)

## Initial setup
- Initialize DVC: `dvc init`
- Configure remote storage (local folder in this project): `dvc remote add -d localstore C:\recomart-dvc-storage`
- Track datasets: `dvc add data/raw data/validated data/prepared data/features data/warehouse`
- Commit metadata to Git and push data to remote: `git commit ...` + `dvc push`

## When new data arrives
1. Run ingestion scripts to create new raw partitions
2. Run validation, preparation, and feature engineering steps to regenerate downstream datasets
3. Run:
   - `dvc add data/raw data/validated data/prepared data/features data/warehouse`
   - `git add *.dvc .gitignore`
   - `git commit -m "Update datasets after new run"`
   - `dvc push`

## Reproducing older versions
- Checkout older Git commit: `git checkout <commit>`
- Restore matching data version: `dvc checkout`
