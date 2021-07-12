# tweet-nlp-text-analyser

## Description:

MSc Project.

An implementation of natural language text analysis on gathered public tweet data. With such methods involving
sentiment analysis techniques and use of neural network classification models.
Such practice methods of machine learning can be used to both determine the
potentiality of depressive mental disorders, as well as, suicidal tendency in the language used by
social media users.

## Top-level Directory:

    .
    ├── api/
    |    ├─store/
    |    |   ├─extraction/
    |    |       ├─ data stream readers
    |    ├─database configuration
    |    ├─data loading
    |
    ├── scripts/
    |    ├─twitter api wrapper
    |    ├─raw data formatting
    |
    ├── data/
    |    ├─raw/
    |    |   ├─completed/ (raw data)
    |    |   ├─process/   (temp)
    |    |   ├─output/    (store ready)
    |    ├─resource utilities
    |
    ├── main.py
    ├── LICENSE
    └── README.md