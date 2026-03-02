#!/usr/bin/env python3
"""
HTTP Service for UDF Compiling API

This service provides an API endpoint that receives Python code snippets
and returns the compiled operator class using the compile() function.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
import sys
import os

# Add the src directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from compiler import (
    BASELINE_REFERENCE_UDF,
    RECOMMENDED_AUTO_CUT_UDF,
    compile_udf,
    infer_line_number_from_code,
)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes


def _error_response(message: str, status_code: int):
    return jsonify({
        'success': False,
        'error': message
    }), status_code


def _compile_operator_class(code: str) -> str:
    line_number = infer_line_number_from_code(code)
    result = compile_udf(code, line_number)
    return result.operator_class


def _validate_post_payload(data):
    if not data:
        return None, _error_response('No JSON data provided', 400)

    code = data.get('code')
    if not code:
        return None, _error_response('No code provided in request', 400)

    if not isinstance(code, str):
        return None, _error_response('Code must be a string', 400)

    return code, None


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'udf-compiling-api',
        'version': '1.0.0'
    })

@app.route('/compile', methods=['POST'])
def compile_code():
    """
    API endpoint to compile Python code into an operator class.
    
    Expected JSON payload:
    {
        "code": "#3\nimport pandas as pd\ndef function(X, Y):\n    X = X + 1\n    return X"
    }
    
    If the first line of code is a comment with a number (e.g., "#3"), that number will be used as the line number to split.
    If the first line is "#baseline", baseline compilation mode will be used.
    
    Returns:
      text/plain operator class code
    """
    try:
        data = request.get_json()
        code, error = _validate_post_payload(data)
        if error:
            return error

        operator_class = _compile_operator_class(code)
        return operator_class
        
    except Exception as e:
        app.logger.exception("Error in compile_code endpoint: %s", e)
        return _error_response(f'Internal server error: {str(e)}', 500)

@app.route('/compile', methods=['GET'])
def compile_code_get():
    """
    GET endpoint for testing - accepts code as query parameter.
    If the first line of code is a comment with a number (e.g., "#3"), that number will be used as the line number to split.
    """
    code = request.args.get('code')
    
    if not code:
        return _error_response('No code provided in query parameter', 400)
    
    try:
        return _compile_operator_class(code)
        
    except Exception as e:
        app.logger.exception("Error in compile_code_get endpoint: %s", e)
        return _error_response(f'Internal server error: {str(e)}', 500)

@app.route('/example', methods=['GET'])
def get_example():
    """Get an example of the expected request format."""

    # Stable examples live under compiler/use_cases.py so docs, API, and tests stay in sync.
    example_code_with_cut = f"#5\n{RECOMMENDED_AUTO_CUT_UDF}"
    example_code_baseline = BASELINE_REFERENCE_UDF
    example_code_auto = RECOMMENDED_AUTO_CUT_UDF

    examples = {
        'example_with_line_cut': {
            'code': example_code_with_cut,
            'description': 'Compile with an explicit cut on line 5 to force process-table split position.'
        },
        'example_baseline': {
            'code': example_code_baseline,
            'description': 'Baseline mode. "#baseline" forces a single process_tables method.'
        },
        'example_auto': {
            'code': example_code_auto,
            'description': 'Recommended default mode. No line prefix; compiler picks the cut automatically.'
        }
    }
    
    return jsonify({
        'examples': examples,
        'instructions': {
            'line_cut': 'Start your code with "#<number>" to specify a line to cut at',
            'baseline': 'Start your code with "#baseline" for baseline compilation (single method)',
            'auto': 'No special first line for automatic optimal cutting'
        }
    })

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.logger.info("Starting UDF Compiling Service...")
    app.logger.info("Available endpoints:")
    app.logger.info("  GET  /health - Health check")
    app.logger.info("  POST /compile - Compile Python code to operator class")
    app.logger.info("  GET  /compile - Test endpoint (use query parameters)")
    app.logger.info("  GET  /example - Get example request formats")
    app.logger.info("Code compilation modes:")
    app.logger.info("  #<number>  - Cut at specific line number (e.g., '#5')")
    app.logger.info("  #baseline  - Baseline compilation (single process_tables method)")
    app.logger.info("  (no prefix) - Auto compilation with optimal cuts")
    app.logger.info("Service will be available at http://localhost:9999")
    
    app.run(host='0.0.0.0', port=9999, debug=True) 
