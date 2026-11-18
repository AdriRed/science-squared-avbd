import matplotlib.pyplot as plt
from matplotlib.cycler
def configure_matplotlib():
    # Configuración estilo revtex4
    plt.style.use('default')  # Empezar desde cero
    # okabe-ito color blind palette
    color_blind_palette = [
        '#E69F00', '#56B4E9', '#009E73', '#F0E442',
        '#0072B2', '#D55E00', '#CC79A7'
    ]

    rcparams = {
        # Fuentes
        'font.family': 'serif',
        'font.serif': ['Times New Roman', 'DejaVu Serif', 'Computer Modern Roman'],
        'font.size': 10,
        'font.weight': 'normal',
        
        # Tamaño de figura (ancho de columna típico de revtex4: ~3.5 pulgadas)
        'figure.figsize': (3.5, 2.8),
        'figure.dpi': 100,
        'figure.facecolor': 'white',
        
        # Ejes
        'axes.labelsize': 10,
        'axes.labelweight': 'normal',
        'axes.linewidth': 0.8,
        'axes.edgecolor': 'black',
        'axes.facecolor': 'white',
        
        # Ticks
        'xtick.direction': 'in',
        'ytick.direction': 'in',
        'xtick.major.size': 4,
        'ytick.major.size': 4,
        'xtick.minor.size': 2,
        'ytick.minor.size': 2,
        'xtick.labelsize': 9,
        'ytick.labelsize': 9,
        
        # Líneas
        'lines.linewidth': 1.5,
        'lines.markersize': 6,
        
        # Leyenda
        'legend.fontsize': 9,
        'legend.frameon': False,
        'legend.loc': 'best',
        
        # Guardado
        'savefig.dpi': 300,
        'savefig.bbox': 'tight',
        'savefig.pad_inches': 0.05,

        'text.usetex': True,
        'text.latex.preamble': '\\usepackage{amsmath, amssymb}\n\\usepackage{siunitx}',
        'pgf.texsystem': 'xelatex'
    }
    plt.rcparams['axes.prop_cycle'] = cycler(color=color_blind_palette)

    plt.rcParams.update(rcparams)
