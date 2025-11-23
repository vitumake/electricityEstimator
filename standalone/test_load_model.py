from tensorflow import keras
from pathlib import Path

model_path = Path(__file__).parent / "models" / "final_nn.keras"
print(f"Trying to load: {model_path}")
model = keras.models.load_model(model_path)
print("Model loaded successfully!")
