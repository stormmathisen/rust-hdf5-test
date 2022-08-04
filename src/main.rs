use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::io::Cursor;
use std::io::{SeekFrom, BufReader};
use chrono::prelude::*;
use chrono::Duration;
use hdf5::{File as HFile, H5Type, Result};
use std::{thread, time};
use std::mem;
use std::fs::File;
use std::io::prelude::*;
use std::sync::mpsc::{channel, Sender, Receiver};


const samples: usize = 512;
const test_file: &str = "/home/storm/Desktop/hdf5rustlocal/example_register";
const adc_offset: u64 = 160;
const adc_length: u64 = 128;
const adc_num: u64= 10;
const runtime: i64 = 1;
const adc_offsets: [u64;  adc_num as usize] = [160, 240, 176, 256, 192, 272, 208, 288, 224, 304];
const active_pulse_offset: u64 = 70;
const total_pulse_offset: u64 = 71;
const state_offset: u64 = 66;


#[derive(Debug)]
struct DataContainer {
    internal_count: u64,
    datetime: DateTime<Utc>,
    active_pulse: u32,
    total_pulse: u32,
    state: u32,
    kly_fwd_pwr: [u16; samples],
    kly_fwd_pha: [u16; samples],
    kly_rev_pwr: [u16; samples],
    kly_rev_pha: [u16; samples],
    cav_fwd_pwr: [u16; samples],
    cav_fwd_pha: [u16; samples],
    cav_rev_pwr: [u16; samples],
    cav_rev_pha: [u16; samples],
    cav_probe_pwr: [u16; samples],
    cav_probe_pha: [u16; samples]
}


fn main() -> std::io::Result<()> {

    //Initialization
    let prog_start = Utc::now();
    let prog_stop = prog_start+Duration::minutes(runtime);
    let reg_file = std::fs::File::open(test_file)?;
    let mut reg_reader = BufReader::new(reg_file);
    let mut internal_counter: u64 = 0;

    //Setup HDF5 thread
    let (datasender, datareceiver) = channel::<DataContainer>();
    let (controlsender, controlreceiver) = channel::<bool>();
    let handler = thread::spawn(|| {
        write_hdf5_thread(controlreceiver, datareceiver);
    });

    //Main Loop
    loop
    {
        reg_reader.rewind()?;
        let shot_start = time::Instant::now();

        let shot_timestamp = Utc::now();

        let mut data_container = DataContainer{
            internal_count: internal_counter,
            datetime: shot_timestamp,
            active_pulse: read_register_offset(active_pulse_offset, &mut reg_reader) as u32,
            total_pulse: read_register_offset(total_pulse_offset, &mut reg_reader) as u32,
            state: read_register_offset(state_offset, &mut reg_reader) as u32,
            kly_fwd_pwr: read_array_offset(samples, adc_offsets[0], &mut reg_reader),
            kly_fwd_pha: read_array_offset(samples, adc_offsets[1], &mut reg_reader),
            kly_rev_pwr: read_array_offset(samples, adc_offsets[2], &mut reg_reader),
            kly_rev_pha: read_array_offset(samples, adc_offsets[3], &mut reg_reader),
            cav_fwd_pwr: read_array_offset(samples, adc_offsets[4], &mut reg_reader),
            cav_fwd_pha: read_array_offset(samples, adc_offsets[5], &mut reg_reader),
            cav_rev_pwr: read_array_offset(samples, adc_offsets[6], &mut reg_reader),
            cav_rev_pha: read_array_offset(samples, adc_offsets[7], &mut reg_reader),
            cav_probe_pwr: read_array_offset(samples, adc_offsets[8], &mut reg_reader),
            cav_probe_pha: read_array_offset(samples, adc_offsets[9], &mut reg_reader)
        };
        let _data_result = datasender.send(data_container).unwrap();
        internal_counter += 1;
        if internal_counter % 100 == 0
        {
            println!("Time elapsed: {} us", shot_start.elapsed().as_micros());
        }
        if shot_timestamp > prog_stop {
            //println!{"{:?}", &data_container}
            break;
        }
        while shot_start.elapsed().as_micros() < 2500{

        }

    }
    let _send_status = controlsender.send(true).unwrap();
    handler.join();
    Ok(())

    /*
    //Preparing everything
    let start: DateTime<Utc> = Utc::now();
    let stop: DateTime<Utc> = start+Duration::minutes(5);
    let hdffile = File::create(&start.format("/home/storm/Desktop/hdf5rustlocal/%Y-%m-%d %H:%M:%S.h5").to_string())?;
    let mut count :u64 = 0;
    
    let array = create_test_buffer();
    let mut test_buffer = Cursor::new(&array[..]);
    loop {
        let utc: DateTime<Utc> = Utc::now();
        let shot_start = time::Instant::now();
        count += 1;
        test_buffer.rewind(); //Rewind buffer to start

        //Read sequentially
        let wave_1 = read_waveform_from_cursor(512, &mut test_buffer);
        //test_buffer.rewind();
        let wave_2 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_3 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_4 = read_waveform_from_cursor(512, &mut test_buffer);
        let wave_5 = read_waveform_from_cursor(512, &mut test_buffer);

        //Store in struct
        let data = FpgaData {
            count: count,
            timestamp: (utc.timestamp(), utc.timestamp_subsec_nanos()),
            wave_1: wave_1,
            wave_2: wave_2,
            wave_3: wave_3,
            wave_4: wave_4,
            wave_5: wave_5
        };
        //Create new group with timestamp as name
        let chunk_size = 256;
        let wave_group = hdffile.create_group(&utc.format("%Y-%m-%d %H:%M:%S.%f").to_string())?;
        let builder = wave_group
            .new_dataset_builder()
            .chunk((chunk_size));
        //let builder = wave_group.new_dataset_builder();
        let ds = builder
            .with_data(&data.wave_1)
            .create("wave_1")?;

        //println!("{:?}", ds);
        //let builder = wave_group.new_dataset_builder();
        let builder = wave_group
            .new_dataset_builder()
            .chunk((chunk_size));

        let ds = builder
            .with_data(&data.wave_2)
            .create("wave_2");
        //let builder = wave_group.new_dataset_builder();
        let builder = wave_group
            .new_dataset_builder()
            .chunk((chunk_size));

        let ds = builder
            .with_data(&data.wave_3)
            .create("wave_3");
        //let builder = wave_group.new_dataset_builder();
        let builder = wave_group
            .new_dataset_builder()
            .chunk((chunk_size));

        let ds = builder
            .with_data(&data.wave_4)
            .create("wave_4");
        //let builder = wave_group.new_dataset_builder();
        let builder = wave_group
            .new_dataset_builder()
            .chunk((chunk_size));

        let ds = builder
            .with_data(&data.wave_5)
            .create("wave_5");
        let dur: [u64; 1] = [shot_start.elapsed().as_micros() as u64];
        let attr = wave_group.new_attr::<u64>().shape([1]).create("shot_duration")?;
        attr.write(&dur)?;
        //println!("shot took {} us", dur[0]);
        if utc > stop {
            break;
        }
        if count % 100 == 0 {
            println!("shot took {} us", dur[0]);
        }
        //if count % 10 == 0 {
            hdffile.flush()?;
        //}
        //TODO: Flush less often
        thread::sleep(time::Duration::from_micros(2500));

    }
    

    Ok(())*/
}

/*fn write_data_hdf5(hdffile: &File, data: FpgaData) -> Result<i32> {
    let write_start: DateTime<Utc> = Utc::now();
    hdffile.create_group(&write_start.format("%Y-%m-%d %H:%M:%S.%f").to_string())?;

    Ok((0))
}*/

fn write_hdf5_thread(controlreceiver: Receiver<bool>, datareceiver: Receiver<DataContainer>) {
    let mut thread_counter = 0;
    loop {
        //println!("Thread is here!");
        thread_counter += 1;
        let received_data = datareceiver.recv_timeout(time::Duration::from_millis(25)).unwrap();
        thread::sleep(time::Duration::from_millis(100));
        let _ctrl = controlreceiver.try_recv();
        if  _ctrl.is_ok() {
            loop {
                let _recv = datareceiver.try_recv();
                if _recv.is_err(){
                    break;
                }
                else
                {
                    let received_data = _recv.unwrap();
                    println!("Still receiving data!!!!")
                }
            }
            println!("{:?}", received_data);
            println!("Quitting after {} loops", thread_counter);
            break;
        }
    }
}

fn read_array_offset(read_len: usize, offset: u64, buffer: &mut BufReader<std::fs::File>) -> [u16; samples] {
    let mut out_buffer: [u16; samples] = [0; samples];
    let _ret = buffer.seek(SeekFrom::Start(offset));
    let _ret = buffer.read_u16_into::<LittleEndian>(&mut out_buffer).unwrap();
    out_buffer
}


fn read_register_offset(offset: u64, buffer: &mut BufReader<std::fs::File>) -> u64 {
    let mut out_buffer: [u64; 1] = [0; 1];
    let _ret = buffer.seek(SeekFrom::Start(offset));
    let _ret = buffer.read_u64_into::<LittleEndian>(&mut out_buffer).unwrap();
    out_buffer[0]
}