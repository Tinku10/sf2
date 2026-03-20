use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong, jobject};
use jni::JNIEnv;

use crate::file::reader::{PlankReader, RecordBatch};
use crate::serde::Serialize;
use crate::types::{data::PlankData, fields::PlankField, types::PlankType};

#[unsafe(no_mangle)]
pub extern "system" fn Java_PlankReader_openNative(
    mut env: JNIEnv,
    _obj: JObject,
    path: JString,
) -> jlong {
    let path: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(e) => {
            env.throw_new("java/io/IOException", e.to_string()).unwrap();
            return 0;
        }
    };
    match PlankReader::open(&path) {
        Ok(reader) => {
            let boxed = Box::new(reader);
            Box::into_raw(boxed) as jlong
        }
        Err(e) => {
            env.throw_new("java/io/IOException", e.to_string()).unwrap();
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_PlankReader_readRowGroupNative(
    mut env: JNIEnv,
    _obj: JObject,
    reader_ptr: jlong,
    id: jint,
) -> jobject {
    let reader = unsafe { &mut *(reader_ptr as *mut PlankReader) };
    match reader.read_row_group(id as usize) {
        Ok(batch) => record_batch_to_jobject(&mut env, batch),
        Err(e) => {
            env.throw_new("java/io/IOException", e.to_string()).unwrap();
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_PlankReader_readRowGroupColumnsNative(
    mut env: JNIEnv,
    _class: JClass,
    reader_ptr: jlong,
    id: jint,
    columns: JObjectArray,
) -> jobject {
    let reader = unsafe { &mut *(reader_ptr as *mut PlankReader) };

    let len = env.get_array_length(&columns).unwrap();
    let col_names: Vec<String> = (0..len)
        .map(|i| {
            let jstr = env.get_object_array_element(&columns, i).unwrap();
            env.get_string(&JString::from(jstr)).unwrap().into()
        })
        .collect();

    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    match reader.read_row_group_columns(id as usize, &col_refs) {
        Ok(batch) => record_batch_to_jobject(&mut env, batch),
        Err(e) => {
            env.throw_new("java/io/IOException", e.to_string()).unwrap();
            JObject::null().into_raw()
        }
    }
}

fn plank_type_to_jclass<'local>(
    env: &mut JNIEnv<'local>,
    plank_type: &PlankType,
) -> JClass<'local> {
    match plank_type {
        PlankType::Str => env.find_class("java/lang/String").unwrap(),
        PlankType::Int32 => env.find_class("java/lang/Integer").unwrap(),
        PlankType::Int64 => env.find_class("java/lang/Long").unwrap(),
        PlankType::Bool => env.find_class("java/lang/Boolean").unwrap(),
        PlankType::Struct(_) => env.find_class("java/util/HashMap").unwrap(),
        PlankType::List(_) => env.find_class("java/util/ArrayList").unwrap(),
    }
}

fn plank_type_to_string(plank_type: &PlankType) -> String {
    match plank_type {
        PlankType::Str => "String".to_string(),
        PlankType::Int32 => "Integer".to_string(),
        PlankType::Int64 => "Long".to_string(),
        PlankType::Bool => "Boolean".to_string(),
        PlankType::List(item) => format!("List<{}>", plank_type_to_string(item)),
        PlankType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{}: {}",
                        f.field_name(),
                        plank_type_to_string(f.field_type())
                    )
                })
                .collect();
            format!("Struct{{{}}}", field_strs.join(", "))
        }
    }
}

fn plank_data_to_jobject<'local>(
    env: &mut JNIEnv<'local>,
    data: &PlankData,
    schema: &PlankType,
) -> JObject<'local> {
    match (schema, data) {
        (PlankType::Str, PlankData::Str(s)) => {
            env.new_string(s).unwrap().into()
        }
        (PlankType::Int32, PlankData::Int32(n)) => {
            let class = env.find_class("java/lang/Integer").unwrap();
            env.new_object(class, "(I)V", &[(*n as jint).into()]).unwrap()
        }
        (PlankType::Int64, PlankData::Int64(n)) => {
            let class = env.find_class("java/lang/Long").unwrap();
            env.new_object(class, "(J)V", &[(*n as jlong).into()]).unwrap()
        }
        (PlankType::Bool, PlankData::Bool(b)) => {
            let class = env.find_class("java/lang/Boolean").unwrap();
            env.new_object(class, "(Z)V", &[(*b as jboolean).into()]).unwrap()
        }
        (PlankType::Struct(fields), PlankData::Struct(values)) => {
            let class = env.find_class("java/util/HashMap").unwrap();
            let map = env.new_object(class, "()V", &[]).unwrap();
            for (field, value) in fields.iter().zip(values.iter()) {
                let key = env.new_string(field.field_name()).unwrap();
                let val = plank_data_to_jobject(env, value, field.field_type());
                env.call_method(
                    &map,
                    "put",
                    "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                    &[(&key).into(), (&val).into()],
                ).unwrap();
            }
            map
        }
        (PlankType::List(item_type), PlankData::List(items)) => {
            let class = env.find_class("java/util/ArrayList").unwrap();
            let list = env.new_object(class, "()V", &[]).unwrap();
            for item in items {
                let val = plank_data_to_jobject(env, item, item_type);
                env.call_method(&list, "add", "(Ljava/lang/Object;)Z", &[(&val).into()]).unwrap();
            }
            list
        }
        _ => JObject::null()
    }
}

fn record_batch_to_jobject(env: &mut JNIEnv, batch: RecordBatch) -> jobject {
    let class = env.find_class("RecordBatch").unwrap();
    let obj = env.alloc_object(&class).unwrap();
    env.set_field(&obj, "rowCount", "I", (batch.row_count as jint).into())
        .unwrap();

    let obj_class = env.find_class("java/lang/Object").unwrap();
    let columns_array = env
        .new_object_array(batch.columns.len() as jint, &obj_class, JObject::null())
        .unwrap();

    for (i, col) in batch.columns.iter().enumerate() {
        let col_obj_array = env
            .new_object_array(col.records.len() as jint, &obj_class, JObject::null())
            .unwrap();
        for (j, record) in col.records.iter().enumerate() {
            let val = plank_data_to_jobject(env, record, batch.schema[i].field_type());
            env.set_object_array_element(&col_obj_array, j as jint, val)
                .unwrap();
        }
        env.set_object_array_element(&columns_array, i as jint, col_obj_array)
            .unwrap();
    }

    env.set_field(
        &obj,
        "columns",
        "[[Ljava/lang/Object;",
        (&columns_array).into(),
    )
    .unwrap();

    let class = env.find_class("java/util/LinkedHashMap").unwrap();
    let schema_map = env.new_object(&class, "()V", &[]).unwrap();

    for col in batch.schema.iter() {
        let field_name = env.new_string(col.field_name()).unwrap();
        let field_type = env
            .new_string(plank_type_to_string(col.field_type()))
            .unwrap();
        env.call_method(
            &schema_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&field_name).into(), (&field_type).into()],
        )
        .unwrap();
    }
    env.set_field(&obj, "schema", "Ljava/util/LinkedHashMap;", (&schema_map).into())
        .unwrap();

    obj.into_raw()
}
