import glob
import json
import os
import sys

import dill
import pandas as pd

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
path = os.path.expanduser('~/airflow_hw/plugins')
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

def predict():
    model_path = f'{path}/data/models'

    # Проверка существования пути к модели
    if not os.path.exists(model_path):
        print(f"Model path does not exist: {model_path}")
        return

    mod = sorted(os.listdir(model_path))
    if not mod:
        print(f"No models found in the path: {model_path}")
        return

    latest_model = mod[-1]
    print(f"Using model: {latest_model}")

    with open(f'{model_path}/{latest_model}', 'rb') as file:
        model = dill.load(file)

    preds = pd.DataFrame(columns=['car_id', 'pred'])

    for file in glob.glob(f'{path}/data/test*.json'):
        with open(file) as fin:
            form = json.load(fin)
            df = pd.DataFrame.from_dict([form])
            y = model.predict(df)
            X = {'car_id': df['id'].values[0], 'pred': y[0]}  # Убедитесь, что 'id' является колонкой в ваших тестовых JSON данных
            preds = preds.append(X, ignore_index=True)

    # Сохранение предсказаний в CSV файл
    predictions_file_path = f'{path}/predictions.csv'
    preds.to_csv(predictions_file_path, index=False)
    print(f"Predictions saved to {predictions_file_path}")

if __name__ == '__main__':
    predict()


