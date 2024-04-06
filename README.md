# Cloud Workload Datasets Repository

This repository contains preprocessed workload datasets related to Google Cloud and Alibaba clusters. The datasets are organized into subfolders for each year and type of workload.

## Folder Structure

- **GC11 (Google Cloud 2011)/**
    - `data/`: Contains data related to Google Cloud workload for the year 2011.
    - `preprocessing_scripts/`: Contains preprocessing scripts for the Google Cloud 2011 data.

- **GC19 (Google Cloud 2019)/**
    - `machine-level/`: Contains data and scripts specific to machine-level workload.
    - `cluster-level/`: Contains data and scripts specific to cluster-level workload.
    Each of which is divided in:
    - `data/`: Contains data related to Google Cloud workload for the year 2019.
    - `preprocessing/`: Contains preprocessing scripts for the Google Cloud 2019 data.

- **AC18 (Alibaba Cluster 2018)/**
    - `data/`: Contains data related to Alibaba cluster workload for the year 2018.
    - `preprocessing_scripts/`: Contains preprocessing scripts for the Alibaba Cluster 2018 data.

- **AC20 (Alibaba Cluster 2020)/**
    - `data/`: Contains data related to Alibaba cluster workload for the year 2020.
    - `preprocessing_scripts/`: Contains preprocessing scripts for the Alibaba Cluster 2020 data.

## Usage
- Clone this repository to your local machine.
- Explore the data and use the preprocessing scripts as needed.

## BibTeX Citation

If you use these preprocessed datasets in a scientific publication, we would appreciate using the following citations:

```
@inproceedings{rossi2022bayesian,
  title={Bayesian uncertainty modelling for cloud workload prediction},
  author={Rossi, Andrea and Visentin, Andrea and Prestwich, Steven and Brown, Kenneth N},
  booktitle={2022 IEEE 15th International Conference on Cloud Computing (CLOUD)},
  pages={19--29},
  year={2022},
  organization={IEEE}
}

@article{rossi2023uncertainty,
  title={Uncertainty-Aware Workload Prediction in Cloud Computing},
  author={Rossi, Andrea and Visentin, Andrea and Prestwich, Steven and Brown, Kenneth N},
  journal={arXiv preprint arXiv:2303.13525},
  year={2023}
}

@inproceedings{rossi2023clustering,
  title={Clustering-Based Numerosity Reduction for Cloud Workload Forecasting},
  author={Rossi, Andrea and Visentin, Andrea and Prestwich, Steven and Brown, Kenneth N},
  booktitle={International Symposium on Algorithmic Aspects of Cloud Computing},
  pages={115--132},
  year={2023},
  organization={Springer}
}
```
