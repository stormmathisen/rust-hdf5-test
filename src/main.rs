use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::io::Cursor;
use std::io::{SeekFrom};
use chrono::prelude::*;
use chrono::Duration;
use hdf5::{File, H5Type, Result};
use std::{thread, time};
use std::mem;


#[derive(Debug)]
struct FpgaData {
    count: i32,
    timestamp: (i64, u32),
    wave_1: Vec<u16>,
    wave_2: Vec<u16>,
    wave_3: Vec<u16>,
    wave_4: Vec<u16>,
    wave_5: Vec<u16>
}

fn main() -> Result<()> {
    //Preparing everything
    let start: DateTime<Utc> = Utc::now();
    let stop: DateTime<Utc> = start+Duration::minutes(5);
    let hdffile = File::create(&start.format("test/%Y-%m-%d %H:%M:%S.h5").to_string())?;

    
    let array = create_test_buffer();
    let mut test_buffer = Cursor::new(&array[..]);
    loop {
        let utc: DateTime<Utc> = Utc::now();
        let shot_start = time::Instant::now();

        test_buffer.rewind(); //Rewind buffer to start

        //Read sequentially
        let wave_1 = read_waveform_from_offset(512, 0, &mut test_buffer);
        //test_buffer.rewind();
        let wave_2 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_3 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_4 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_5 = read_waveform_from_cursor(512, &mut test_buffer);

        //Store in struct
        let data = FpgaData {
            count: 0,
            timestamp: (utc.timestamp(), utc.timestamp_subsec_nanos()),
            wave_1: wave_1,
            wave_2: wave_2,
            wave_3: wave_3,
            wave_4: wave_4,
            wave_5: wave_5
        };
        //Create new group with timestamp as name
        let wave_group = hdffile.create_group(&utc.format("%Y-%m-%d %H:%M:%S.%f").to_string())?;
        let builder = wave_group
            .new_dataset_builder()
            .chunk((512));
        let ds = builder
            .with_data(&data.wave_1)
            .create("wave_1")?;

        //println!("{:?}", ds);
        let builder = wave_group.new_dataset_builder();
        let ds = builder
            .with_data(&data.wave_2)
            .create("wave_2");
        let builder = wave_group.new_dataset_builder();
        let ds = builder
            .with_data(&data.wave_3)
            .create("wave_3");
        let builder = wave_group.new_dataset_builder();
        let ds = builder
            .with_data(&data.wave_4)
            .create("wave_4");
        let builder = wave_group.new_dataset_builder();
        let ds = builder
            .with_data(&data.wave_5)
            .create("wave_5");
        let dur: [u64; 1] = [shot_start.elapsed().as_micros() as u64];
        let attr = wave_group.new_attr::<u64>().shape([1]).create("shot_duration")?;
        attr.write(&dur)?;
        println!("shot took {} us", dur[0]);
        if utc > stop {
            break;
        }
        hdffile.flush()?;
        //TODO: Flush less often
        thread::sleep(time::Duration::from_micros(2500));

    }
    

    Ok(())
}

fn read_waveform_from_offset(read_len: usize, offset: u64, buffer: &mut Cursor<&[u8]>) -> Vec<u16> {
    let mut out_buffer = vec![0; read_len];
    let _ret = buffer.seek(SeekFrom::Start(offset));
    let _ret = buffer.read_u16_into::<LittleEndian>(&mut out_buffer).unwrap();
    out_buffer
}

fn read_waveform_from_cursor(read_len: usize, buffer: &mut Cursor<&[u8]>) -> Vec<u16> {
    let mut out_buffer = vec![0; read_len];
    //let _ret = buffer.seek(SeekFrom::Start(offset));
    let _ret = buffer.read_u16_into::<LittleEndian>(&mut out_buffer).unwrap();
    out_buffer
}

fn create_test_buffer() -> Vec<u8> {
    let mut buffer = vec![];
    let mut counter = 0;
    while counter < 5120 {
        for i in 0..300 {
            buffer.write_u16::<LittleEndian>(i).unwrap();
            counter += 2;
        }
    }
    buffer
}