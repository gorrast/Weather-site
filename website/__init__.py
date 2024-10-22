from flask import Flask



def create_app():
    app = Flask(__name__)
    
    from .views import views
    
    
    return app
    
    