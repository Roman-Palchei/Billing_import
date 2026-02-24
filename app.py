from flask import Flask, render_template, request, flash, redirect, url_for
from utils.importer import data_import

app = Flask(__name__)

app.secret_key = 'random_secret_key_change_this_later'

@app.route('/', methods=['GET', 'POST'])
def home():
    result_message = None
    preview_data = None
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            result_message = "None file in query"
        else:
            file = request.files['csv_file']
            if file.filename == '':
                result_message = "File not change"
            else:
                importer = data_import()

                preview_data = importer.get_preview(file)

                if not preview_data:
                    result_message = "File red as empty! Check delimiter"
                else:
                    result_message = "File red. Look table to down"
                try:
                    result_message = importer.import_data_from_file(file)
                except Exception as e:
                    result_message = f"Critical error: {e}"

    return render_template('index.html', message=result_message, preview_data=preview_data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)