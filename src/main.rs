use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::prelude::*;
use std::io::Cursor;
use std::io::{SeekFrom, BufReader};
use chrono::prelude::*;
use chrono::Duration;
use hdf5::{File as HFile, Group, H5Type, Result};
use std::{thread, time};
use std::mem;
use std::fs::File;
use std::io::prelude::*;
use std::sync::mpsc::{channel, Sender, Receiver};


const samples: usize = 512; //Number of samples in each array
const test_file: &str = "/home/storm/Desktop/hdf5rustlocal/example_register"; //Location of test-register
const adc_offset: u64 = 160; //Offset to first ADC array
const adc_length: u64 = 128; //Offset between ADC arrays
const adc_num: u64= 10; //Number of ADCs
const runtime: i64 = 1; //Runtime in minutes
const adc_offsets: [u64;  adc_num as usize] = [160, 240, 176, 256, 192, 272, 208, 288, 224, 304]; //Offsets for each ADC
const active_pulse_offset: u64 = 70; //Offset for active pulse registry
const total_pulse_offset: u64 = 71; //Offset for total pulse registry
const state_offset: u64 = 66; //Offset for stat
const chunk_size: usize = 512; //HDF5 chunk size


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
}

fn write_data_hdf5(hdffile: &HFile, data: DataContainer) {
    let write_start = time::Instant::now();
    let timestamp = &data.datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string();
    let hdfgroup = hdffile.create_group(timestamp).unwrap();

    //Write attributes

    let attr = hdfgroup.new_attr::<u64>().shape([1]).create("internal_count").unwrap();
    attr.write(&[data.internal_count]).unwrap();

    let attr = hdfgroup.new_attr::<u64>().shape([1]).create("active_pulse").unwrap();
    attr.write(&[data.active_pulse]).unwrap();

    let attr = hdfgroup.new_attr::<u64>().shape([1]).create("total_pulse").unwrap();
    attr.write(&[data.total_pulse]).unwrap();

    let attr = hdfgroup.new_attr::<u64>().shape([1]).create("datetime").unwrap();
    attr.write(&[data.datetime.timestamp_nanos()]).unwrap();

    let ds_builder = hdfgroup
        .new_dataset_builder()
        .chunk((chunk_size));
    let ds = ds_builder
        .with_data(&data.kly_fwd_pwr)
        .create("kly_fws_pwr");

    let write_dur: [u64; 1] = [write_start.elapsed().as_micros() as u64];

    let attr = hdfgroup.new_attr::<u64>().shape([1]).create("write_duration").unwrap();
    attr.write(&write_dur).unwrap();
}

fn write_hdf5_thread(controlreceiver: Receiver<bool>, datareceiver: Receiver<DataContainer>) {
    let mut thread_counter = 0;
    let start: DateTime<Utc> = Utc::now();
    let hdffile = HFile::create(&start.format("/home/storm/Desktop/hdf5rustlocal/%Y-%m-%d %H:%M:%S.h5").to_string()).unwrap();

    loop {
        //println!("Thread is here!");
        thread_counter += 1;
        let received_data = datareceiver.recv_timeout(time::Duration::from_millis(25)).unwrap();
        write_data_hdf5(&hdffile, received_data);
        if thread_counter % 100 == 0
        {
            hdffile.flush();
        }
        let _ctrl = controlreceiver.try_recv();
        if  _ctrl.is_ok() {
            let _recv = loop {
                thread_counter += 1;
                let _recv = datareceiver.try_recv();
                if _recv.is_err(){
                    break;
                }
                else if _recv.is_ok()
                {
                    write_data_hdf5(&hdffile, _recv.unwrap());
                    println!("Still receiving data!!!!")
                }
            };
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