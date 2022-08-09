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
//const TEST_FILE: &str = "/home/storm/Desktop/hdf5rustlocal/example_register"; //Location of test-register
const BAR1_FNAME: &str = "/home/storm/Desktop/hdf5rustlocal/pcie_bar1_s5";
const DMA_FNAME: &str = "/home/storm/Desktop/hdf5rustlocal/pcie_dma_s5";
const NAS_LOC: &str = "/home/storm/Desktop/hdf5rustlocal/"; //Location to move hdf5 files at midnight
const ADC_OFFSET: u64 = 160; //Offset to first ADC array
const ADC_LENGTH: u64 = 128; //Offset between ADC arrays
const ADC_NUM: u64= 10; //Number of ADCs
const RUNTIME: i64 = 10*60; //Runtime in minutes
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
    //These track when the program started and how long it will run for (currently minutes)
    let prog_start = Utc::now();
    let prog_stop = prog_start+Duration::minutes(RUNTIME);

    //This is the register the code reads from. In the production version, it would be two registers (DMA and BAR2)
    let dma_file = File::open(DMA_FNAME)
        .with_context(|| format!("Failed to open {}", DMA_FNAME))?;
    
    //This is a buffered interface to the open file
    let mut dma_reader = BufReader::new(dma_file);

    let bar1_file = File::open(DMA_FNAME)
        .with_context(|| format!("Failed to open {}", DMA_FNAME))?;

    //This is a buffered interface to the open file
    let mut bar1_reader = BufReader::new(bar1_file);



    //This is the internal shot counter. At 400Hz this is good for >a billion years
    let mut internal_counter: u64 = 0; 

    //Setup HDF5 thread
    //The channel is a multi producer, single consumer FIFO buffer. Only one thread can receive, but multiple threads can put data in the channel
    //We use two, one to send data to the writing thread and one that relies on optimistic receive succeeding to tell the write thread to close
    let (datasender, datareceiver) = channel::<DataContainer>();
    let (controlsender, controlreceiver) = channel::<bool>();

    //This spawns a thread that does hdf5 writing (using a closure)
    let handler = thread::spawn(|| {
        write_hdf5_thread(controlreceiver, datareceiver);
    });

    //Main Loop
    loop
    {
        //Rewind the cursor of the registry file 
        dma_reader.rewind().context("Failed to rewind file!")?;
        bar1_reader.rewind().context("Failed to rewind file!")?;
        //Store an Instant for faking the rep rate and a DateTime for timestamping the data with the system clock
        let shot_start = time::Instant::now();
        let shot_timestamp = Utc::now();
        //Store the data from the register as a structure we can transmit over a channel
        let data_container = DataContainer{
            internal_count: internal_counter,
            datetime: shot_timestamp,
            active_pulse: read_register_offset(ACTIVE_PULSE_OFFSET, &mut bar1_reader) as u32,
            total_pulse: read_register_offset(TOTAL_PULSE_OFFSET, &mut bar1_reader) as u32,
            state: read_register_offset(STATE_OFFSET, &mut bar1_reader) as u32,
            kly_fwd_pwr: read_array_offset(ADC_OFFSETS[0], &mut dma_reader),
            kly_fwd_pha: read_array_offset(ADC_OFFSETS[1], &mut dma_reader),
            kly_rev_pwr: read_array_offset(ADC_OFFSETS[2], &mut dma_reader),
            kly_rev_pha: read_array_offset(ADC_OFFSETS[3], &mut dma_reader),
            cav_fwd_pwr: read_array_offset(ADC_OFFSETS[4], &mut dma_reader),
            cav_fwd_pha: read_array_offset(ADC_OFFSETS[5], &mut dma_reader),
            cav_rev_pwr: read_array_offset(ADC_OFFSETS[6], &mut dma_reader),
            cav_rev_pha: read_array_offset(ADC_OFFSETS[7], &mut dma_reader),
            cav_probe_pwr: read_array_offset(ADC_OFFSETS[8], &mut dma_reader),
            cav_probe_pha: read_array_offset(ADC_OFFSETS[9], &mut dma_reader)
        };
        //Transmit data over the FIFO channel
        let _data_result = datasender.send(data_container).unwrap();
        //Here we increment the internal counter and print the time elapsed (using the Instant, creating a Duration) every 100 shots
        internal_counter += 1;
        if internal_counter % 100 == 0
        {
            println!("Time elapsed: {} us", shot_start.elapsed().as_micros());
        }
        //Check if we're meant to stop and break out of the loop if we are
        if shot_timestamp > prog_stop {
            //println!{"{:?}", &data_container}
            break;
        }
        //This is the fake rep rate, blocking until 2500 microseconds has passed since the start
        //I'm not sure how the FPGA communicates a trigger interrupt to the CPU card, but this can also just look for changes in the FPGA pulse count register
        while shot_start.elapsed().as_micros() < 2500{

        }

    }
    //Once we break out of the loop, we send a shutdown command to the HDF writing thread
    let _send_status = controlsender.send(true)
        .context("Failed to shut down thread with control channel")?;
    //And then join it to wait for it to finish anything it was doing
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
        .and_hms(0,0, 0);
    
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
                std::fs::remove_file(&hdffname);
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

        if thread_counter % 10 == 0
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
            let copy_thread = thread::spawn(move || {
                std::fs::copy(&hdffname, NAS_LOC.to_owned()+&hdffname);
                println!("Finished copying file!");
                std::fs::remove_file(&hdffname);
            }
            );

            copy_thread.join();
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