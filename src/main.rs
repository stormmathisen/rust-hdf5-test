//std imports
use std::io::prelude::*;
use std::io::{SeekFrom, BufReader};
use std::{thread, time};
use std::fs::File;
use std::sync::mpsc::{channel, Receiver};

use chrono::prelude::*;
use chrono::Duration;
use byteorder::{LittleEndian, ReadBytesExt};
use hdf5::{File as HFile};

use anyhow::{Result, Context};


//TODO: External config file
//TODO: Error handling at all .unwrap()

const SAMPLES: usize = 512; //Number of samples in each array
const TEST_FILE: &str = "/home/storm/Desktop/hdf5rustlocal/example_register"; //Location of test-register
const NAS_LOC: &str = "/home/storm/Desktop/hdf5rustlocal/"; //Location to move hdf5 files at midnight
const ADC_OFFSET: u64 = 160; //Offset to first ADC array
const ADC_LENGTH: u64 = 128; //Offset between ADC arrays
const ADC_NUM: u64= 10; //Number of ADCs
const RUNTIME: i64 = 1; //Runtime in minutes
const ADC_OFFSETS: [u64;  ADC_NUM as usize] = [160, 240, 176, 256, 192, 272, 208, 288, 224, 304]; //Offsets for each ADC
const ACTIVE_PULSE_OFFSET: u64 = 70; //Offset for active pulse registry
const TOTAL_PULSE_OFFSET: u64 = 71; //Offset for total pulse registry
const STATE_OFFSET: u64 = 66; //Offset for stat
const CHUNK_SIZE: usize = 512; //HDF5 chunk size
const DATA_FIELD_NAMES: [&str; ADC_NUM as usize] = [
    "kly_fwd_pwr",
    "kly_fwd_pha",
    "kly_rev_pwr",
    "kly_rev_pha",
    "cav_fwd_pwr",
    "cav_fwd_pha",
    "cav_rev_pwr",
    "cav_rev_pha",
    "cav_probe_pwr",
    "cav_probe_pha"
];


#[derive(Debug)]
struct DataContainer {
    internal_count: u64,
    datetime: DateTime<Utc>,
    active_pulse: u32,
    total_pulse: u32,
    state: u32,
    kly_fwd_pwr: [u16; SAMPLES],
    kly_fwd_pha: [u16; SAMPLES],
    kly_rev_pwr: [u16; SAMPLES],
    kly_rev_pha: [u16; SAMPLES],
    cav_fwd_pwr: [u16; SAMPLES],
    cav_fwd_pha: [u16; SAMPLES],
    cav_rev_pwr: [u16; SAMPLES],
    cav_rev_pha: [u16; SAMPLES],
    cav_probe_pwr: [u16; SAMPLES],
    cav_probe_pha: [u16; SAMPLES]
}

impl IntoIterator for DataContainer {
    type Item = [u16; SAMPLES];
    type IntoIter = std::array::IntoIter<Self::Item, 10>;

    fn into_iter(self) -> Self::IntoIter {
        let iter_array = [
            self.kly_fwd_pwr,
            self.kly_fwd_pha,
            self.kly_rev_pwr,
            self.kly_rev_pha,
            self.cav_fwd_pwr,
            self.cav_fwd_pha,
            self.cav_rev_pwr,
            self.cav_rev_pha,
            self.cav_probe_pwr,
            self.cav_probe_pha
        ];
        iter_array.into_iter()
    }
}


fn main() -> Result<()> {

    //Initialization
    let prog_start = Utc::now();
    let prog_stop = prog_start+Duration::minutes(RUNTIME);
    let reg_file = File::open(TEST_FILE)
        .with_context(|| format!("Failed to open {}", TEST_FILE))?;
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
        reg_reader.rewind().context("Failed to rewind file!")?;
        let shot_start = time::Instant::now();

        let shot_timestamp = Utc::now();

        let data_container = DataContainer{
            internal_count: internal_counter,
            datetime: shot_timestamp,
            active_pulse: read_register_offset(ACTIVE_PULSE_OFFSET, &mut reg_reader) as u32,
            total_pulse: read_register_offset(TOTAL_PULSE_OFFSET, &mut reg_reader) as u32,
            state: read_register_offset(STATE_OFFSET, &mut reg_reader) as u32,
            kly_fwd_pwr: read_array_offset(ADC_OFFSETS[0], &mut reg_reader),
            kly_fwd_pha: read_array_offset(ADC_OFFSETS[1], &mut reg_reader),
            kly_rev_pwr: read_array_offset(ADC_OFFSETS[2], &mut reg_reader),
            kly_rev_pha: read_array_offset(ADC_OFFSETS[3], &mut reg_reader),
            cav_fwd_pwr: read_array_offset(ADC_OFFSETS[4], &mut reg_reader),
            cav_fwd_pha: read_array_offset(ADC_OFFSETS[5], &mut reg_reader),
            cav_rev_pwr: read_array_offset(ADC_OFFSETS[6], &mut reg_reader),
            cav_rev_pha: read_array_offset(ADC_OFFSETS[7], &mut reg_reader),
            cav_probe_pwr: read_array_offset(ADC_OFFSETS[8], &mut reg_reader),
            cav_probe_pha: read_array_offset(ADC_OFFSETS[9], &mut reg_reader)
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
    let _send_status = controlsender.send(true)
        .context("Failed to shut down thread with control channel")?;
    let _handler_status = handler.join();
    Ok(())
}

fn write_data_hdf5(hdffile: &HFile, data: DataContainer) -> Result<()> {
    let write_start = time::Instant::now();
    let timestamp = &data.datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string();
    let hdfgroup = hdffile.create_group(timestamp).unwrap();

    //Write attributes

    let attr = hdfgroup
        .new_attr::<u64>()
        .shape([1])
        .create("internal_count").unwrap();
    attr.write(&[data.internal_count]).unwrap();

    let attr = hdfgroup
        .new_attr::<u64>()
        .shape([1])
        .create("active_pulse").unwrap();
    attr.write(&[data.active_pulse]).unwrap();

    let attr = hdfgroup
        .new_attr::<u64>()
        .shape([1])
        .create("total_pulse").unwrap();
    attr.write(&[data.total_pulse]).unwrap();

    let attr = hdfgroup
        .new_attr::<u64>()
        .shape([1])
        .create("datetime").unwrap();
    attr.write(&[data.datetime.timestamp_nanos()]).unwrap();

/*     let ds_builder = hdfgroup
        .new_dataset_builder()
        .chunk((CHUNK_SIZE));
    let ds = ds_builder
        .with_data(&data.kly_fwd_pwr)
        .create("kly_fws_pwr");
 */    
    //TODO: Test!!!
    let mut n = 0;
    for wave in data {
    let ds_builder = hdfgroup
        .new_dataset_builder()
        .chunk(CHUNK_SIZE);

    let _ds = ds_builder
        .with_data(&wave)
        .create(DATA_FIELD_NAMES[n])
        .with_context(|| format!("Failed to write dataset {}", DATA_FIELD_NAMES[n]))?;
        n += 1;
    }

    let write_dur: [u64; 1] = [write_start.elapsed().as_micros() as u64];

    let attr = hdfgroup.new_attr::<u64>()
        .shape([1])
        .create("write_duration")
        .context("Failed to create attribute: Write duration")?;

    let attr = attr
        .write(&write_dur)
        .context("Failed to write attribute: Write duration")?;
    Ok(attr)
}

fn write_hdf5_thread(controlreceiver: Receiver<bool>, datareceiver: Receiver<DataContainer>) {
    let mut thread_counter = 0;
    let start: DateTime<Utc> = Utc::now();
    let mut next_day = start
        .date()
        .succ()
        .and_hms(0,0,0);
    
    let mut hdffname = start
        .format("%Y-%m-%d %H:%M:%S.h5")
        .to_string();
    let mut hdffile = HFile::create(&hdffname)
        .unwrap();

    loop {
        //println!("Thread is here!");
        let now = Utc::now();
        //Check if past midnight, create new file if yes
        //TODO: Implement move old file to network storage!
        if now > next_day {
            next_day = now
                .date()
                .succ()
                .and_hms(0,0,0);
            
            hdffile.close();

            let copy_thread = thread::spawn(move || {
                std::fs::copy(&hdffname, NAS_LOC.to_owned()+&hdffname);
                println!("Finished copying file!");
            }
            );

            hdffname = now
                .format("%Y-%m-%d %H:%M:%S.h5")
                .to_string();

            hdffile = HFile::create(&hdffname)
                .unwrap();
            }
        thread_counter += 1;
        let received_data = datareceiver
            .recv_timeout(time::Duration::from_millis(25)).unwrap();

        let mut _write_res = write_data_hdf5(&hdffile, received_data)
            .with_context(|| format!("Failed to write hdf5 at {:?}", now));

        if thread_counter % 100 == 0
        {
            let _flush_res = hdffile.flush().context("Unable to flush hdf5 file");
        }

        let _ctrl = controlreceiver.try_recv();
        if  _ctrl.is_ok() {
            let _recv = loop {
                thread_counter += 1;
                let _recv = datareceiver
                    .try_recv();
                if _recv.is_err(){
                    break;
                }
                else if _recv.is_ok()
                {
                    _write_res = write_data_hdf5(&hdffile, _recv.unwrap())
                    .with_context(|| format!("Failed to write hdf5 file at closing"));
                    println!("Still receiving data!!!!")
                }
            };
            println!("Quitting after {} loops", thread_counter);
            break;
        }
    }
}

fn read_array_offset(offset: u64, buffer: &mut BufReader<std::fs::File>) -> [u16; SAMPLES] {
    let mut out_buffer: [u16; SAMPLES] = [0; SAMPLES];
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