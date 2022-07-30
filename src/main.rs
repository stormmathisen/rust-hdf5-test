use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::io::Cursor;
use std::io::{SeekFrom};
use chrono::prelude::*;
use hdf5::{File, H5Type, Result};


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
    //println!("Hello, world!");
    let utc: DateTime<Utc> = Utc::now();
    //println!("{:?}", utc);
    let array = create_test_buffer();
    //println!("{}", array.len());
    let mut test_buffer = Cursor::new(&array[..]);
    let n = 2700;
    let _ret = test_buffer.seek(SeekFrom::Start((n-1)*2));
    let read = test_buffer.read_u16::<LittleEndian>();
    //println!("{:?}", read);
    //println!("The {}th element is: {:?}", n, read);
    let _ret = test_buffer.rewind();
    /*let wave_1 = read_waveform_from_offset(512, 4*512, &mut test_buffer);
    let wave_2 = read_waveform_from_offset(512, 3*512, &mut test_buffer);
    let wave_3 = read_waveform_from_offset(512, 2*512, &mut test_buffer);
    let wave_4 = read_waveform_from_offset(512, 1*512, &mut test_buffer);
    let wave_5 = read_waveform_from_offset(512, 0*512, &mut test_buffer);
    */
    let wave_1 = read_waveform_from_cursor(512, &mut test_buffer);
    let wave_2 = read_waveform_from_cursor(512, &mut test_buffer);
    let wave_3 = read_waveform_from_cursor(512, &mut test_buffer);
    let wave_4 = read_waveform_from_cursor(512, &mut test_buffer);
    let wave_5 = read_waveform_from_cursor(512, &mut test_buffer);
    let data = FpgaData {
        count: 0,
        timestamp: (utc.timestamp(), utc.timestamp_subsec_nanos()),
        wave_1: wave_1,
        wave_2: wave_2,
        wave_3: wave_3,
        wave_4: wave_4,
        wave_5: wave_5
    };
    println!("{}", test_buffer.position());
    //println!("{:?}", data);
    let hdffile = File::create("test_hdf5.h5")?;
    //println!("{:?}", hdffile);
    let wave_group = hdffile.create_group("waves")?;
    let builder = wave_group.new_dataset_builder();
    let ds = builder
        .with_data(&data.wave_1)
        .create("wave_1");
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