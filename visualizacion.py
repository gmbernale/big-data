import matplotlib.pyplot as plt
from collections import defaultdict
import time

counts = defaultdict(int)
plt.ion()  # Modo interactivo

while True:
    # Aquí iría el código para obtener los últimos conteos de Spark
    # Simulamos datos para el ejemplo
    for event in ['login', 'purchase', 'logout', 'view', 'click']:
        counts[event] += random.randint(0, 3)
    
    plt.clf()
    plt.bar(counts.keys(), counts.values())
    plt.title('Eventos en tiempo real')
    plt.ylabel('Cantidad')
    plt.pause(5)  # Actualizar cada 5 segundos
