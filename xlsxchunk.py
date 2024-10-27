import pandas as pd

def clean_text(df):
    df = df.replace('á','a', regex=True)
    df = df.replace('Á','A', regex=True)
    df = df.replace('é','e', regex=True)
    df = df.replace('É','E', regex=True)
    df = df.replace('Í','I', regex=True)
    df = df.replace('í','i', regex=True)
    df = df.replace('ó','o', regex=True)
    df = df.replace('Ó','O', regex=True)
    df = df.replace('ú','u', regex=True)
    df = df.replace('Ú','U', regex=True)
    return df

def merge_columns_with_names(df: pd.DataFrame) -> pd.Series:
    
    def combine_row(row):
        filtered_items = {k: v for k, v in row.items()}
        
        combined = ", ".join([f"{col}: {str(val).strip()}" for col, val in filtered_items.items()])
        return combined
    
    return df.apply(combine_row, axis=1)


def chunk_dataframe_text(df, max_chunks, overlap):
    if max_chunks is None or max_chunks == 0:
        return df
    result_df = df.copy()
    
    def combine_row(row):
        return ', '.join([f"{col}: {str(val)}" for col, val in row.items() if pd.notna(val)])
    
    result_df['combinado'] = result_df.apply(combine_row, axis=1)
    
    def split_text_into_chunks(text, max_chunks, overlap):
        words = text.split()
        if len(words) <= max_chunks:
            return [text]
        
        chunks = []
        current_position = 0
        
        while current_position < len(words):
            end_position = min(current_position + max_chunks, len(words))
            
            current_chunk = ' '.join(words[current_position:end_position])
            chunks.append(current_chunk)
            
            if end_position < len(words):
                current_position = end_position - overlap
                
                remaining_words = len(words) - current_position
                if remaining_words < max_chunks // 2:
                    chunks[-1] = ' '.join(words[current_position - overlap:])
                    break
            else:
                break
        
        return chunks

    new_rows = []
    next_index = 0 
    
    for _, row in result_df.iterrows():
        text = row['combinado']
        chunks = split_text_into_chunks(text, max_chunks, overlap)

        if len(chunks) == 1:
            row_dict = row.to_dict()
            row_dict['original_index'] = next_index
            new_rows.append(row_dict)
            next_index += 1
        else:
            for chunk in chunks:
                row_dict = row.to_dict()
                row_dict['combinado'] = chunk
                row_dict['original_index'] = next_index
                new_rows.append(row_dict)
                next_index += 1
    
    final_df = pd.DataFrame(new_rows)
    final_df = final_df.reset_index(drop=True)
    
    if 'original_index' in final_df.columns:
        final_df = final_df.drop('original_index', axis=1)
    
    return final_df


def df_to_row_json(df):
    json_data = {}
    
    for i, row in df.iterrows():
        chunk_key = f'chunk{i+1}'
        

        row_dict = {}
        for column, value in row.items():
            if pd.isna(value):
                value = None
            elif isinstance(value, (pd.Timestamp, pd.DatetimeIndex)):
                value = value.strftime('%Y-%m-%d')
            elif hasattr(value, 'item'):
                value = value.item()
                
            row_dict[column] = value
        
        json_data[chunk_key] = row_dict
    
    return json_data

def chunkgenerator(path, max_chunks, overlap):
    df = pd.read_excel(path)
    df = clean_text(df)
    df['Combinado'] = merge_columns_with_names(df)
    df_resultado = chunk_dataframe_text(df, max_chunks, overlap)
    json_r  = df_to_row_json(df_resultado)
    return json_r