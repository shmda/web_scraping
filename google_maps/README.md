# Containerised Project

This project is a containerised Python application designed for Google Maps location search scraping and data processing, or possibly automation. 

It runs inside a lightweight Podman/Docker container and automatically handles data stored in the `/data` directory.

## 📂 Project Structure
.
├── main.py         # Main application script
├── requirements.txt # Python dependencies
├── data/           # Directory for input/output data
└── README.md       # Project documentation

## 🚀 Features
- Containerised with **Podman/Docker** for easy deployment  
- Automatic handling of data in `/data`  
- Simple to set up and run  

## ⚙️ Setup

### Prerequisites
- Podman or Docker installed  
- Python 3.10+ (only needed if you want to run locally without containers)  

### Build the Container
podman build -t my-containerised-project .


### Run the Container
podman run --rm -v ./data:/app/data my-containerised-project

This will mount your local `./data` folder into the container at `/app/data`.

## ▶️ Usage

1. Place your input files into the `data/` directory.
2. Run the container (see above).
3. Output will be generated in the same `data/` directory.

Example:

```bash
podman run --rm -v ./data:/app/data my-containerised-project
```

---

## 📌 Notes

* All configuration can be managed through `main.py`.
* No external APIs (e.g., Google Maps) are required.
* Logs and errors will be printed to the console.
