```python
395| @app.route('/documents', methods=['GET', 'POST'])
396| @login_required
397| def documents():
398|     if request.method == 'POST':
399|         client_id = request.form.get('client_id') if current_user.role == 'admin' else current_user.client_id
400|         uploaded = request.files.get('file')
401|         if not uploaded or not uploaded.filename:
402|             flash('No file selected for upload.', 'warning')
403|             return redirect(url_for('documents'))
404| 
405|         if not allowed_file(uploaded.filename):
406|             flash('Unsupported file type.', 'danger')
407|             return redirect(url_for('documents'))
408| 
409|         original_filename = secure_filename(uploaded.filename)
410|         saved_name = f"{uuid4().hex}_{original_filename}"
411|         target = UPLOAD_DIR / saved_name
412| 
413|         try:
414|             # Save the uploaded file
415|             uploaded.save(target)
416|             file_path = f'static/uploads/{saved_name}'
417| 
418|             # Insert document details into the database
419|             execute_db(
420|                 '''INSERT INTO documents(client_id, document_name, original_filename, file_path, tax_year, status, notes, uploaded_by)
421|                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
422|                 (
423|                     client_id, request.form.get('document_name'), original_filename, file_path,
424|                     request.form.get('tax_year'), request.form.get('status') or 'Received',
425|                     request.form.get('notes'), current_user.id
426|                 )
427|             )
428| 
429|             flash('Document successfully uploaded.', 'success')
430|         except Exception as e:
431|             flash(f'An error occurred while uploading the document: {str(e)}', 'danger')
432|             app.logger.error(f'Error uploading document: {e}')
433|             return redirect(url_for('documents'))
434| 
435|         return redirect(url_for('documents'))
436| 
437|     rows = query_db(
438|         '''SELECT d.*, cl.name AS client_name
439|            FROM documents d JOIN clients cl ON cl.id=d.client_id
440|            WHERE (?='admin' OR d.client_id=?)
441|            ORDER BY d.created_at DESC, d.id DESC''',
442|         (current_user.role, current_user.client_id or -1)
443|     )
444|     clients_rows = query_db('SELECT * FROM clients ORDER BY name') if current_user.role == 'admin' else []
445|     return render_template('documents.html', documents=rows, clients=clients_rows)
```