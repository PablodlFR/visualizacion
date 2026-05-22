from plotnine import ggplot, aes, geom_line, geom_point, theme
import pandas as pd

def generar_plot(df):
    df = df[df['TERRITORIO#es'] == 'Canarias']
    df = df[df['MEDIDAS#es'].isin(['Sueldos y salarios', 'Pensiones', 'Prestaciones por desempleo'])]
    
    plot = (ggplot(df, aes(x='TIME_PERIOD_CODE', y='OBS_VALUE', color='MEDIDAS#es', group='MEDIDAS#es')) 
            + geom_line() 
            + geom_point()
            + theme(figure_size=(10,5))
            + ggtitle('Evolución de las principales fuentes de renta en Canarias')
            + xlab('Año')
            + ylab('Porcentaje (%)')
            + labs(color='Tipo de renta'))
    
    return plot