from PIL import Image
import numpy as np


def create_img(img_folder='/tmp/airflow-images/', dots=10):
    a = np.random.rand(dots, dots, dots)
    img = Image.fromarray(a, mode='RGB')
    img_path = f'{img_folder}/img_{dots}_{datetime.now().strftime("%Y%m%d%H%M%S")}.png'
    img.save(img_path)
    return img_path

