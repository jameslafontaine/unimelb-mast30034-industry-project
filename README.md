[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/wNN69YNG)

## IMPORTANT
If you get the error ModuleNotFoundError: No module named 'module_name', try changing python version on bottom right. 

Run the command `pip install -r requirements.txt` in your terminal to install any python libraries required to run the code in this repository.

Run the command `pip freeze > requirements.txt` in your terminal to create a new requirements.txt to push to the repository if you ever need to install more libraries.

# Generic Buy Now, Pay Later Project README.md
|       Name        |  Student ID |
| ----------------- | ----------- |
| James La Fontaine | 1079860     |
| Jack Olivier 		| 1269795 	  |
| Harry Fisher 		| 1082897	  |
| Dulan Wijeratne	| 1181873	  |
| Nivethan Iyer		| 1171632     |


#####

**Research Goal:** Our goal is to generate a robust ranking system with insights to assist a BNPL firm in deciding which merchants they should accept a partnership with.

In the process we will be answering the following 2 questions:
1. What are the 100 best merchants according to our ranking system?
2. What features or heuristics did we discover that greatly separated which merchants should and shouldn't be accepted?

**Timeline:** The timeline is February 2021-October 2022 based on the transaction data currently available.

To run the pipeline, please visit the `scripts` directory and `notebooks` directory as instructed:
1. `notebooks/analysis/preliminary/` notebooks perform the preliminary analysis we did at the start of the project.
2. `scripts/ETL.py`: Run with `python3 scripts/ETL.py` from the root directory. This automatically extracts, transforms and loads the data into the `./data/curated/` directory. (The ETL-Breakdowns have the same content as the ETL script just split up into smaller sections).
3. `notebooks/analysis/nulls&missing/Null_&_Missing_Analysis.ipynb` performs the null value and missing value analysis, but the actions we decided to take from this analysis were added to the ETL script.
4. Run `notebooks/analysis/distribution_outlier/distribution_analysis.ipynb` then `outlier_removal.ipynb` to perform outlier analysis and removal.
5. Run the notebooks in `notebooks/analysis/insights/` in the order listed within the README.md in that folder to generate insights, then run `feature_engineering.ipynb` to perform feature engineering.
6. Run `notebooks/analysis/ranking_system/ranking_system.ipynb` to create the rankings and associated visualisations.

Note that there is a summary of much of this process in `notebooks/summary.ipynb` alongside ours assumptions and limitations for this project.


Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details pertaining to the datasets.