mod dispatcher;
pub mod memtable;
mod sstable;

use crate::engine::memtable::{MemTable, SsTableSize};
use crate::wal::Wal;
use crate::{Responder, Result, Storage, WalStorage};
use bytes::Bytes;
use dispatcher::Dispatcher;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

/// This is where data files will be stored.
// TODO: Make configurable.
// TODO: Move to storage may be.
pub const DATA_PATH: &str = "/var/lib/bureau";

/// This is how many tables are allowed to be in the process of writing to disk at the same time.
/// Grow this number to make DB more tolerant to high amount writes, it will consume more memory
/// in return.
const DISPATCHER_BUFFER_SIZE: usize = 32; // TODO: Make configurable.

// TODO: Make configurable.
const MAX_KEY_SIZE: u32 = 512; // 512B.

// TODO: Make configurable.
const MAX_VALUE_SIZE: u32 = 2048; // 2KB.

#[derive(Debug)]
pub enum Command {
    Get {
        key: Bytes,
        responder: Responder<Option<Bytes>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        // Having an optional responder here allows to issue 'fire-and-forget' set commands.
        responder: Option<Responder<()>>,
    },
    Shutdown {
        responder: Responder<()>,
    },
}

#[derive(Debug)]
pub struct Engine<W: WalStorage> {
    input_rx: mpsc::Receiver<Command>,
    memtable: MemTable,
    wal: Wal<W>,
}

/// Engine is a working horse of the database. It holds memtable and a channel to communicate commands to.
impl<W: WalStorage> Engine<W> {
    pub fn init(rx: mpsc::Receiver<Command>, wal_storage: W) -> Result<Self> {
        let (wal, initial_records) = Wal::init(wal_storage)?;
        if initial_records.is_some() {
            info!("Engine started with initial records recovered from WAL");
        }
        let mt = MemTable::new(SsTableSize::Default, initial_records);

        Ok(Engine {
            input_rx: rx,
            memtable: mt,
            wal,
        })
    }

    /// This function is to run in the background thread, to read and handle commands from
    /// the channel. It itself also spawns a dispathcher thread that works with everything
    /// living on the disk. Thats why storage is being passed here, hense it is not 'belong'
    /// to engine directly.
    pub async fn run<T: Storage>(mut self, storage: T) -> Result<()>
    where
        <T as Storage>::Entry: Send,
    {
        let (disp_tx, disp_rx) = mpsc::channel::<dispatcher::Command>(64);
        let disp = Dispatcher::init(disp_rx, DISPATCHER_BUFFER_SIZE, storage.clone())
            .map_err(|e| format!("could not initialize dispatcher: {}", e))?;

        let dispatcher_join_handle = tokio::spawn(async move {
            match disp.run().await {
                Ok(()) => {
                    tracing::info!("dispatcher stoped");
                }
                Err(e) => {
                    tracing::error!("dispatcher exited with error: {:?}", e);
                }
            };
        });
        let dispatcher_abort_handle = dispatcher_join_handle.abort_handle();

        let disp_storage = storage.clone();
        let compaction_disp_tx = disp_tx.clone();
        let compaction_join_handle = tokio::spawn(async move {
            match dispatcher::compaction::run(disp_storage, compaction_disp_tx).await {
                Ok(()) => {
                    tracing::info!("dispatcher stoped");
                }
                Err(e) => {
                    tracing::error!("dispatcher exited with error: {:?}", e);
                }
            };
        });
        let compaction_abort_handle = compaction_join_handle.abort_handle();

        while let Some(cmd) = self.input_rx.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    match self.get_from_mem(&key) {
                        Some(value) => {
                            let _ = responder.send(Ok(Some(value)));
                        }
                        None => {
                            let _ = disp_tx
                                .send(dispatcher::Command::Get { key, responder })
                                .await;
                        }
                    };
                }
                Command::Set {
                    key,
                    value,
                    responder,
                } => {
                    if let Err(err) = validate(&key, &value) {
                        responder.and_then(|r| r.send(Err(err)).ok());
                        continue;
                    }

                    match self.memtable.probe(&key, &value) {
                        memtable::ProbeResult::Available(new_size) => {
                            if let Err(e) = self.wal.append(key.clone(), value.clone()) {
                                error!("could not append wal entry: {}", e);
                                continue;
                            };
                            self.memtable.insert(key, value, Some(new_size));
                            responder.and_then(|r| r.send(Ok(())).ok());
                        }
                        memtable::ProbeResult::Full => {
                            // Swap tables and respond to client first.
                            let old_table = self.swap_table();
                            if let Err(e) = self.wal.rotate() {
                                // This database is not to run without WAL since it's LSM and bunch of data lives
                                // in memroty, hence any restart without working WAL will lead to data loss.
                                // Thats why we return here shutting down engine with error.
                                error!("could not rotate WAL: {}", e);
                                return Err(e.into());
                            };
                            if let Err(e) = self.wal.append(key.clone(), value.clone()) {
                                error!("could not append wal entry: {}", e);
                                continue;
                            };
                            self.memtable.insert(key, value, None);
                            responder.and_then(|r| r.send(Ok(())).ok());

                            // Now send full table to dispatcher to put it to disk.
                            let (resp_tx, resp_rx) = oneshot::channel();

                            let _ = disp_tx
                                .send(dispatcher::Command::CreateTable {
                                    data: old_table,
                                    responder: resp_tx,
                                })
                                .await;

                            let _ = resp_rx.await; // Blocks if dispatcher tables buffer is full.
                        }
                    }
                }
                Command::Shutdown { responder } => {
                    let (disp_shutdown_rx, disp_shutdown_tx) = oneshot::channel();
                    let _ = disp_tx
                        .send(dispatcher::Command::Shutdown {
                            responder: disp_shutdown_rx,
                        })
                        .await;
                    let _ = disp_shutdown_tx.await?;
                    self.wal.flush()?;

                    let _ = responder.send(Ok(()));
                    dispatcher_abort_handle.abort();
                    compaction_abort_handle.abort();
                    return Ok(());
                }
            };
        }

        let _ = dispatcher_join_handle.await;
        let _ = compaction_join_handle.await;

        Ok(())
    }

    /// It only checks hot spots: cache, memtable.
    fn get_from_mem(&self, key: &Bytes) -> Option<Bytes> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        None
    }

    /// Swaps memtable with fresh one and sends full table to dispatcher that syncronously write it to disk.
    fn swap_table(&mut self) -> MemTable {
        let mut swapped = MemTable::new(SsTableSize::Default, None);
        std::mem::swap(&mut self.memtable, &mut swapped);
        swapped
    }
}

fn validate(key: &Bytes, value: &Bytes) -> crate::Result<()> {
    if key.is_empty() {
        return Err(crate::Error::from("key is empty"));
    }

    if key.len() > MAX_KEY_SIZE as usize {
        return Err(crate::Error::from("key is too long"));
    }

    if value.is_empty() {
        return Err(crate::Error::from("value is empty"));
    }

    if value.len() > MAX_VALUE_SIZE as usize {
        return Err(crate::Error::from("value is too long"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem;
    use crate::wal::mem_storage::{InitialState, MemStorage};
    use rand::{rng, Rng};
    use tracing::debug;
    use tracing_test::traced_test;

    static DATA: [&str; 337] = [
        "#+VsKVS&d=7nVxtak2kohq@wxk&g6dQ6yLacn_zu1Cp7CgvWI21aI~=bIDVwGs^8=r*58F6@JXVEn867d0r&m-DVd+0DptveqK0m",
        "#^K0aAC4hbAldT16t=fHJvxo0o8FSVwt&0iNeA&6*5J#VLy9Ogmg^9vdkXdFkc9SMIuvSX@kQDuUF4fl=p31SK0hX~BRveYxTiki",
        "#cfr0iHCypr^ft8#TLPz0*A-=#K@Rg*t^*nPMy=qg&yEhn=k@oVYfPfowFpGnqQPVrut4O#gnO4w3Zg8^88Pa7m&ADH57dHBkX*_",
        "#oGvDTzDWxB0cODxzE07um2d38750tzVMeiYkkXTgoXG3UN#iVn6Hsyd6LULDAlYEQEQ-U-JZK=l=CN=7NouOuIg=C9K#SlBa9sq",
        "#p+g@xDEkAJz3IV0v3UwXQW8*9qI3HCi5CHxx1RC-BVIankO7dqWON&eGPigz~*pyJyEdigAo*4lsE0sPFQY+Wv+wOC#m8geKslf",
        "&Cr9vRXso5*CI72lpRIIeasFDSS9BaysWYwB&yqWD5OY8HWGgn4ptWDYZ1=dgxgGf4W0PgIT0a3Ah#rNOBa*S6hsNez862d3YD~B",
        "&He*gZAUHF1yO+IQgpicbc2JT=Hlq3Ig&drZqI9+=hhxbn6+KvEtJ#H=~Ko=NQa0FyESsNNa#drkg4UeKZWLrE5T4*9_FE=H7oiv",
        "&a3r8~HQJHAbdOcCEbkJu@bdabEG3uz7dGvgnu@~=CfqEeyyXlpKtsp___uTe+iozncS1JmNuZyX8UoLItvTf+tlgWoq3XKv4K-=",
        "&p~95DKAEtu0lfx97erTHAun9gzXE5Ws7gy2QNV*lKe-+JNcYG5lm9Rq6_Dcwy^h*f_ERJG6CiVLxRKq@cJRrlpTRHBJPb_h+5Uf",
        "*MZ9Y_YwHb5DgWSh6RR3pcl*34QLnX7zQbs*hVo2puPvp1T3focgJqBwxPziBlsz7aqb~YX3Iu6EVZg1gt2XTYnTcwNmg#g9aroL",
        "*z6DsLqlgzZgLqAsQl7wWrRozH@pPh-A3F4w=wLaDksCFy7QImpzOMhNNwBi#VTIY7WqZW@M_GA^6Hk+S-=8=nq@-Kt#ZG^~epFQ",
        "+AAJH&OCfvH4nJNy8bo&2GvJr7rHDuKw#c4J~cL2=zMIpP#6-N-iYnRkHypid90gGq-xKJGPNEorcygOGa=~6g+lSWwUVQQwJUXh",
        "+KFBzfSpsLb3MAI+6*~eOeL9g-YAi+zSLIy0ET0^Eoae~WBi700J_Llz+qr5C#elzbihTD&@PNr3_KPNsmGN#4HiVF3KR6ZX*-^y",
        "+hZHz-9Eyl42Wth-znKk4vz6Q8sRcn*oRfIO0Q~cVAs0Lt7M=Fpg+gaGgsmzv3bWat2gASoUB-Lxpe&Syo~qWUaZg9g3a8Rv#EyP",
        "+p7gbUB86HlDfSWQswpOxmvgEK74Q8BkwNRkfeY@K@_cJw9d-AB*5Ek7^gmuz^Jd^lfKK+_Dl^kIVR-z7CXMn@d~L2GOre#_9T=C",
        "+zMtCV2WUF1+H7++46liK-D2+0rMk+VImCn8S2P47df5Ox^0BFy~WRT8eQNwP^dt9Adi5dpMHOQR*MP4=d1OPN&@GO*TT7Ox7LK9",
        "-&9Fv1k023vr-Byb@^xRoDRFRPWQrT&WodveFZwEq6BPLVHfX7+lOXF^hrpOrgJ&f8R=r-Ivz#xSQiMZao-ly7RzeLmWYUmvrtM0",
        "--UmyxCX6#cEW931WzH=Klv#nc2^Xug^lcz=G1fwY~nKBh*wBN6Xes22Ik&EUm8mo-dlspE_Ha=0_n0^kJUuuvtvBBmNeV^Zw9p2",
        "-SKnJr6N~00l=u+Z_gHgMICE@AOX@@l1Yezi04y^^m-AGuA=XhFA~nZ~YH~gtABbbiyfR=xWq06-wO2Vt=F~4ppgZ^1CK@2o8ZzW",
        "-WvILY*Sc0xhl+u+MCHtlvZyuZ7*R2TZWwISVgWT1KY9kW7K_L-BVF&NVSAx+DAFDg=oOTp6DHk&#uD_fh91bk3Vg2DrRXa7QnJU",
        "-g6nM-sf~BXFr0&DDh+0i6sLPuUxmtDBNO#Zsy7-opP~xTiQ^VN=ThLBn-yyl&uhMBZ~@DA4OgL@Ugs3FIQq=ma^zx&gOr1yA&F9",
        "-pbua@c#LfTI3TB@f6~JG0GeK8VXsCuQ1WTxw3VZVEy81uHIZgsirOTM#e=Z0Iy=Tg2NzuolMqUvC6=JU55vIcuZuAo2V^-T@=H=",
        "0OfCir1h&s_gIHHC_*0GRQXbo79kRZeLPYXbaVmcsaogY#o18HD869_mks+8Ls7cH4xOscP&-01OOY2=QJ*1Mt&+COcXcuCKP@hZ",
        "0TJYGB+&xaTS&YhBmRe7JeMK+KOrzMdep@QLpR@FGD7R#7rNCwp8qn@8J-2VGwK#E3ThBpR3Kxg24d-XcVK6po3#XK84C1i1CP8n",
        "0a8M15+hw_vVHYAlpvEy+U2ZdZgKMLwd~7MKV15uNu*MaBbN=wQrryHhaYawdr~VYnL5LeVusu=4Go-16KNqbRCk8oF4vE@Bl_kL",
        "0dh70zV5lu#Ny9UWU9hpsVIr1g6CmH8dCg2Em+LR~t1sRQrQWxuDDuIq7AMctP#*FP@=^U0uqg78tMcOC5i5zRyGmnav~g^R3EI~",
        "1N-A_xoGQZ7NM~Qa=w9-kO#9&W97LbTs73BJ=_+5MmEqO2EUaVhvMusFhH7mo1TT6KR^zv+r=SK^9moYWxJe83s&yi34In4S95ls",
        "1NzKi3P5kq2fpD4rE5~Wqcfay~Gu@U=IvV5+DhN4Oc=JUlf=+0xlbHH=wuFc52ZItUMvg@k8vl4gUPKt#^c225Nse#H_5agggBtW",
        "1bTt-BzF7fVXZaBrFwOqdJXrnBs8YR3T2@kdJ8n76gao4gPeNxAv_rb3826-4tN~xe+hYNGBoq&6ygqQqZJTpON4LYgeX*y*u*0S",
        "1g&DgTs&gz59QquGKQD=CtmwpgDoZ48muIJfNU8Ug7Z1EMeW&O3zd53&wGkRmSHexLdDwV^cU56D1HgsRoAaYPzt@cMpVAYKc8LZ",
        "1lgANGns5_GlQcvBq4MT-rIQkuJgl--iQQLLSr*HWsJHz_~WTB@rkZlZ5dTgQ5QZZS+OqGJ_^2DZ5oikUiDdmC8aBUUa~4ub5aI~",
        "1pIa6#cf+~KdrEpPehsYfAs+G+suRi2BX27c2XvgW*&qFR2ggTFQHEc=pxwUlp27a&YcYGk-6iHl@Ko~0yT2vkeKngwgo0ycyoH^",
        "2050s#kUbMu5rz+YrKf@RMY0I9w-kI6#B#z0q&W^SzU~rPzVI#@2SwbxDMfbWc*dzXB#A1a2FirEr_P0u3g1lmhw7&Tt=9iWIE+Q",
        "20eK1n9hA5fKfGt3mBdvi^9rMK7S4AhY5BL=BVS44oLOiyhdfg4Pvp&+^=O0w9=en6IffDb939lr4DwUwA4wDxC-B=RlyiQOHDKU",
        "26-xU5BsP*M23gXoDgaFpJOa=bUqzPk8QUy+gK&ffqP4_h+uQRmwu4J#3qTldqOJbO#piR_NQ@guB@*t2C+tcanc*xAR#T_C3mKL",
        "2B1d+V*VG3J*JKZ~cRL61N*l_DLiopZKfwkagHY1ya@Je9XI7ay0D_2L+LJrkhe5Tt&_6dYA5kKxsGiF51LikpV0yAgum8MyUcPV",
        "2^SC&MY^gt8=AwnOxcpZVBmlRKuxKEg3hspEubVzbO&UGLFWxd^bTFx5Z_r6b6@G~GfnAz&9GH5R_fgm*8o9-^2iwIgosGTVYY7B",
        "2ga~L1PFBrxtk&25q8fDzu1wIZlgIHkm_~5JsZ2-3pgCN^gScgAdiDnUs8skQoSL@&0bxP2NgpKXg0gX~ovyF9TSI0QmC*g0dFkJ",
        "2w^vH*gk^SSgnLdI@6FPrq1dVuUrF1T51=i*9wEudD385i*9ICa9ZVt4~HXlWUcErU+FFWM4LGHelUayrepzqcNEQ#=qg6NPAVz2",
        "2yx&w#akpBNATcNHIx6HPcxxl5^L~^dcs=d0_+A7@oqh3GaPZb~yZ6EiHM#uGUE&gaJyA2Pm7X0rU*+3m__ha0o=k~xHHP_TTgBW",
        "3CeNgPA#0xQZMo=QSeoKC2NnWWOgUR&HSUeUntdkY#&8F1iF_w*TXMDp#H#U0ADH6KoGEePX7QNg7JZ-0RszF4MqxJdV#kaQVCd4",
        "3dvLwIqgV=m*H#KWuEQz&hSXyUxyWXVNh6k#o~GBRTDoIhyHeJhggTi*B2xby#T=u-MWHqQXFkXdQ8XzRwf*e0gyqTotHrDnYM3p",
        "3ea#0MITtGFA9*-U@#R8T*IrT^tyvRVH5#skuG6IJZ0_++_0V=PyTdVfo0e5FG+hSziJONXU2^vCuImhko~kx^@fgm*g0kXDTIF1",
        "41@9QCy33JDe~vcUn5GzV@5*VYK*lYM@yeFuzNzmG4m_LTyn&YF&4pvwSsfcozKnzf@@W4s7Dkd0qsZIu=oMBM3De-fF1snvdouI",
        "4LI#p3kfJk_Hk9hRgSx2&t5H2b-Su^g5mTNC6gLl9f-l~C+CegpgsnZgCVLv*Jm8=uQtxs#0e+BsX-xkm~zh7JgwYYMuB*UQMu_r",
        "4UKQzC6IYTIDbgXh=7oL0~kdgg8kyT~E-ElzZOZivIAgCzu2haJqIzsIcqhoaCkHgaNFI5~TTAZwWio8x5wmKf&d#QvLX0tkZG2R",
        "4sbXYhr3&EVZOQ2D&ku-+V*&ldKgg-hWSbHpV=zoqSXs+^iX*oR&kQHczg+ynRMy6a~so2P_CJV61JA5VXRIC*cCD@EmZbPxVTu2",
        "5-=vK2eZOAo0gK^Al@C-JD=kgXnK0FO^V~Dduk4v2mKVMH0s8r2xdXaS21*wZtFMpGGenrxqqx75LTCAsD-w~lmeGuTXlfK9Vrk4",
        "54VIKWPYzO7mP7=c9HA0U5euL4LoTpw&FG3bbBaEB4DJ^2C~IhJ6JG*9H-hp-0MC#J~W&~&7HJidtb43g40z=qpRDLs81JH^AgxK",
        "5@Zw0h7c+wf3yXk=opx0kQsw=3T~qA25pnpgo+RTOVzSa_xg1DIvxQmF9ubgBK^eSGz+aFuT9K2~VNy6qJ=k2@3Bsk*_olB^7vpU",
        "5Kk*+4+OegiSVigQRPZELF4xt4UCe1^NwcHPg4^hvB6P-kz#_GwQ6M4db-OfmKp#fB*BaSnOyCcBauu8aHlDGJ*ehEYO0mdEQDGP",
        "5d~Pv1~gEpSc=*#Ngp1_d_DOf27pRE2ish~H+~9qnFVuXWvmEBM^#rQ7_JGJgxxr6c5zN5b&=J_p^nKenbDfHODh=MJ2NgN3wR4x",
        "5eqMvzn8&4_eYR@b00c9t0L#Ti9xc79-9bGTIkZ*VXCMH^QW@Inn#@Unt8fMDr^6H+YtIU4i^ZE~TWe5qsOL&@sZ#^I_KBy54DHP",
        "5kW+K=tnL~g-D~u2NdB3vi-3o0lkuL~~5vGXhJvxY_eNBMP#&JMDoGFZ6_6ou760fyYlUoeQZfuYtcx20-Am6gH6*=_tEuiJIy*N",
        "5r@*b6rMCEbRu~hSz308e1W4QWigP8mMn_C5b8ttA8EU1lGu9yTnheyU5xV^g0VX8WXNtrEgoYIV&=qwv7@XzbDx#1qFcVmoBZ~-",
        "5vJ3p-mrn^Kp#6AiZ+oCaM9LzzgtEQxdgeF^+cODu=ZJw2Tsn1FH0I~va4cQqVXW#gHxQi3zvQ^-Yye=qK48@d4Hx&IZ-#&kbBVY",
        "60mxeEATaiXGfVt_V3^Z_URSfrZA*icJ8VfYG2geWrBI=4V2cn~56Ml7kVYVbOMdvJDrzoSemts3B^mXfknm1tp^PJvImuz#DIVk",
        "66sTWvg_s4K8y5PFcrhWmzv=&K~HG__F*n2R6xiJAHPX6i7*TzT23g5K0cF^S1OUNKo9zv5i=CO4Tk4M88lg-dVpq9~EfFUzpAf6",
        "68apYCAByRopE-fL_c*_9I9U&1-yeK_D~1U7NY~k@yguW#e=kgMwvOlTx8MZpi~v_yUz3fD6EcOaass^BhP9_igoD1M+MpgFsqWV",
        "6EL7=RVkB6&tX^A=T1Uw3vkQ89Q=-I@c@qSyao3Ip1p1ZRWMBxlm*OkwWgDJbWsdgC*MAnxx8Sx3PmsHy&Z6K^v5Zr~FbZkrVbgX",
        "6EogckSW3r20nC-LEraR~I9mEHiDX_ARAYk2J8cQ7JIPf^3sV01Kg#J7A#@iLoud4kQHrUb&pJh3wtCQ+xoVwVZXKQHm0ki4Nfd@",
        "6IG&WGuNJh~QX4v4FJ&RxhB@~2WFC7gUl^z3CwlP+kExRRPaYyzhW~KPyuDQMVQ6HcdCPG7RF4S-Z+F+U=R5~GOpO&MIx0&-kisV",
        "6o8q5BDvbYrGirDBrtCxXe@T6Q6ZTo+0Ix8xa9OEuzi6S10XsZ*@Cq_3wc0h5_UrE9RPPTlcw+ZXOM&aU4gL0W+dsfErRC^fdlHw",
        "7+_6s6xoJyLoy40Tzlfto1lR&VA0YyAbq=&u@E8#e@Z8cpV4r*A6c2e4C6BQucx-HofCxyg4QrxLSywtgHC95@CXu_BT~Ztn9^nh",
        "74+CYgmG3QRVfiTwgCSXEi1p_G1uwFxe0eZdFQ&aWma9q^B&h4+IZeZt42V14hvYo0WOgKb6TkKd@PtL~pgr_ct3vwD8QyVU#NKi",
        "76NFXuVl0hpYMZ@Ur00fdZoL@+xm*Ym#P=kYTCz#ef0HfIApxYN64_@1f~pGb~UGP#t4@r^m=&gdEUWy~oy1Zf_3Hv0fcZFrd0aw",
        "77tG=thgXA_IylsCfLQ^MUT1dGcF304CJdq8gOxcK3kUsEvPOTkUTCW4YuLCg-G&UHA-5^mt#hqP+1uNfgLH9i6tzss5wAhW_b&P",
        "7QSMzfDq0oxGmfmPELCZ2^tE8z=1m*s9Bp80~MV4kDfV*#o+ramRPt9dl&LA4no^9AXxMUp2w&-aa&lPTMzd1SJ*@bvPEqO*6Iyt",
        "7fxu3_UsYv&BOfFitgomWrJEsSoTKigP#lV9oH3Dq~b3W-xVk#p+u2Jd@KR8l4=+m+0-QzAtvoFqPzqwbR#=kI-80Mdi#org4HZJ",
        "7h7R&E2#bHxdpn-EFonyBo3ngYH&OGuts&U+ME@ZzRgs-IU7yFyr_cYJ~gtYw~&+H8x75EqWudFo+5X7YT2Ws0Ukn-fPLkeg3k6o",
        "7i*kF4bS86uMWr3^D*GXM5G*U_yDAiy6+@3&3gszvAQeN&bq34-uEv2mYcqqHAgUbegiiIu@C7HCYQAz+aLLVDKTd9osnXyhrLzB",
        "7rJ=dsKqyBNlpB47or&yYsohRUk_^=&Y4T5RfFhhaVX@LhW*UtYfBUVbaq=*N3WR+uJ9gUd3pp0NIg#AnWIH02&w~2OkTSyPbagA",
        "8@GqbgGKVA72JUS#agCr5aoPhQPxKwr8qe5-GO^9caDgrFV&^l4hI-@IcEff8KR*M0gO-pB0qphOAQ-=@h4&Mz^rIdBpPnC3pOhk",
        "8HVlFi2nJImT9yPYOZ#gKUl@X~mDs&rPq&hnXaf0r1WV2KS^Y2449yr@v&=8AcBp38xvH^s25nwPE~Sd#DO76SwyEIZh8**lQZ&L",
        "8MFoSmlnYTRR1BqwuHtMC4dCCuZVSVLJC^Dl7htG~3UXvNN*rrQltvA0R3_p7kMPl+gi4BbGisEyHNo4gCRVHIFiHpPIegM7OqvX",
        "8R*gRS~_6WygoKg4sJS3yKKpT2-VOoS8br2@owCO~&HCiprvwYhvTrQu&F7MGfzfG0lg0v_pP*ShH04FRLa-2_Zs9*tN3mZq^GII",
        "8U9YK_O5QFMq5RLK&gc0&qb@7rk6T*~XdPBH2vzNpoJcQgSD@gg7#4M-_~tdXuNI4HJwROA=MLPvy*Cad^X*x5^3RI1RysONm-Tn",
        "8m~N-GIQna3OsdBi80aAHq#BVEmPd9fU&czNIIa#dn0U0TvgLPw@HrRdZrsc~8fz2&T3fL-PMeCYyBnk9=lN_*+giABN4rS1K8vd",
        "8wDWGA+Bq0k7xZtL=hGp5~4do0#tg1tlW~LUYf-GP7x6N5_ctoJ3eA-YM_VQ~l@n0gpI4ZexgIagEdgvlg@-7zYmKeVBDhy+@zfY",
        "8xC=Zgiy+=pRYz1b#7rg3h&fLHq^LUoiW&3ao56QwwpeNN-mdT2X+Pi&qg^xVg=Oamg^ruWhkrb4-o0+&^eBf2Vl87-blWg88bV~",
        "9=axcMX~weybRt&zk@@AP~S9yW#2y~Rx491wmPUal@WHF_YUllUkfL_Q*t_@LI_gTXPg@w7Jx#@IaJo^=5Q-VBtiE#aqBPg~ecnE",
        "9bz0#UhTr&tvhFNRU=#9+1qEsE1QK9FEa0G#v-UQsgELNFN4&RcHkRbT8@C&BXuRAYE~rcLt9Bqsk3R3Rt@O3mK5QLQA@tyH6^7T",
        "@OoPxYWlH_G~-HmPCfOPvmpEkFkah_sed3aVx=IVJ*EuPEboHM_kBdtWpG7pyCQi*6c55W&YIoxId8=tzT@3pIC2OPqW^64QAUnH",
        "@zHkVST-^wALfYlX0p1+z&cJ8+IbTPow_kzwyy*O67adfpxar5zDCld79MbYg&c5TZ4PXg1hfMSBn2c@Z0=X9czUBdwJ&D&_fVD9",
        "A0pgqp##HZ_6#Mz2eCFKT4CvpqZr@r-=qt#8_r-RtE#&G4XBvc+1_6lh&AClHJ5Ffdr7utCCcwLEcyK4tEw_YgOqCk*rtF*pQMOB",
        "AJXFvJS_sRXfXtSZ_DwO9N0g-FuqOlXO771U1nAr8etD8m0-d4xi_x0*ZzOgcKRVHe4bDLtycPW_fpf5GG8#*-I^X&ET9sD_Z7@*",
        "ASOg8WXhrU62d0JbdHdVX9tOf*b^nupkWsf2p16Fh7zzgnNzKCDzFcLt@r3Bobm9e279tULBCFdVIz-NMmCLiCXbR~A*C@raiPvI",
        "Ad5NfoCCg~CZg4UxzEhOTeMLLe&i^QM_w8qAvVuwxq7yYfW_RpEhkX#wK+ffA9Goo*0C*cKXo8VXpXZPXfmgRbaQt=c9gmx3Fk0U",
        "Ak=cZkJLBPZUsXDw1~SOskFd&L-fM8bRwKUgKKPPd0k48eIoynOxPTg*T1f~_6&kJi0MqJ1lgDIm4y~vpX^Xg&vP2myOsb^6Z4Ku",
        "AkEUNg5QKHt4MLVxsb_#CGAELSYIgncyVZoXpFXIIthPaC+-~SuAXBRiXi_FUOgm-QAqG+U^tr-Mvk=@HF6lee0ZQnfTAHgN5f7E",
        "BWRuy-w&vTbOv6Qw&6p#m5dB0MTcJ-U~@x6=DWm6-NnbVAq4F9iS^SDu~i6MQWJL^GCyLlXcF5WwJ67z9_3T2l6AAgE-FkCMcCTo",
        "BXpyI_TCpddRnGqUHxSBoLUQME74yXXJBaBD0T7LJ&WS2=i^qtPDdayMm~0nC~bmS_GbEI=r6SmX+SASV6f-i&CF7_T1zKYhXWvD",
        "BfRE=h11C075Q~4Q3XXggFOkX2Ex@OQ#5y^hGHXTA5BO6+OhfJtfal~FioABPUtkSPZNMY28&4cwPbtPF*cKC_aee~tVJ~-VPFWH",
        "BvgMLn3c^Lk@OgVmQIIY~-@GA^UMiI&ZS_n5v7^UvE0_EAP9dOhVpa8N^Q**xhiYqfg=#~~hdL9G#sJect-q_EgN&U&DMkbc2NQx",
        "BxPRY#L+U^W+c7NTaogB23OuYZLGQ7XhW4gLtZy+=0a*emD+XQYpgVG7&QtSuidQWmW=EqyB0PBmO-h+MZ^Xha5BgEEJNn7Y+GqH",
        "Bxtzt7Y+v=sBKb=XxnxDHNYPVX*@gzn#TGgDaK@Y7~AT6=n@lZw6dcGG_UL9h3h3^MLy2=^&M2KIDvFLR5_*&n+o24Y=F#ONBzJr",
        "ByA@7#NrfI5CVK3e3viy5ZZM6CN53&qp+gLSi7YF8xrtqxCXTz8s#OXKvFdX+xl7E4DNVss7L&ya*tge86CkS1utMe=@1cw-KV8J",
        "B~y#GyIB&NX7gT1t-A6HkY#r7t6w9X+aw0kqt-M*hOrPzpft_Ku#Y-zNNYkStDTCimQvBtl*Uf9Pp^*VBVrCr~gNLylos&8^dgyz",
        "C*U5~o7zQiSZgCVzPSn#YD30XFxe*HAMNu-P=8AnY3D&g113B31z*VJcSMQqo^I^fe0ANse84Kp0egJhI2GuR&aFSp&P-qJ^1znO",
        "C+l_3=~YmT1XgP*gLbpvumoI@WbzE3f4U7ZFsVOztA2IX#2Lgbg1L#B^lAHIq9E=^VXiuG1bVZGn0LS=nV&VEhbo0KEZBDPztANt",
        "C+vGKJaq^8tP1@1rBpU4+ePR*3Yo9QWD@s4qBAL^_UkVrIGNgZmbHCP1o*PNYQVCdH&y#^o1&^w=PJNXPiPWpu4fITIJ_z5BR0Jz",
        "CSL+X8~WQT_m_wv@PMo1_W9Q9hRaIxUXFy6mX#uYReQ2hucsV&svi3m5K5_c&-66YArsyyrOorUWaq-9Z^wDOUZDbFHz+WOJuoaA",
        "C^UQk1P~OwmvsCbRH1cggZG3csl4sHEF*N#yLtK^H=gCfD32dFKZWr6g4+2Nfy-M*43mABg+h*16*9z-C^o1rx32U~MDSP_HEy++",
        "Cg-Nh^VDM+Vt6B=q^5xybx+-bH+S@aa*EU63I6qv4MQ@oxeEwS=yGkZy@n@E*6S+k-LX-hNPGdXsM_a8iSPLCmT^CNRP9goH^ytn",
        "DR18@yGgKaEG@w#Q*5msPRmtypoP=a^5Pll+Foikan4X9T^RGm-aybbOHF^HT86evl+QP15cSViGx9c9cclfzw+EyxRO-DIqI~TV",
        "DdWIYmCm~0YdQQhFqrVUgP7Z^55oed~CAAgvcO#^vpz6rOqXN66yMAplFC5_ac#1PPfl_gtZ6E*lI9zff7u36F8u6qhD_gPI4i^g",
        "Dt_Y1SJ=~q-70-=&@qUnq&n2EfYJZLXnLEMQSN&l3phEQgpDpe3+vQhG5m*VzL&r5JSHtW89vlEFJO2gYVP0C4wUeS_7HEthD2w5",
        "E++2wK0-8sF^0XqCx1JTlaIo59H=P+st@@HwCh2cZg&W+EDf-1IPYEsxIRri8+CR6h*dh8l@Wf&1#_~tOe_buR5mJ8fw*BwmXEMx",
        "E5Xe6iUw#2O0lkMggoIGZ8yeg=QL0dc=WRHsXaSEQr2VlG^PflKc5+Z=7=QlqiO7=3a@=RwLvC=ON7KUO#zBGW=Oz@@C&PWOtW66",
        "E8MK1XtnIvyYxv7Yi@e4#I0@*KlcnWPAwWlhJLh3w5x+ErumlzmTIhK48gTMcpNlFg61C&#6Idgtz5#1tERBe9xlNC7#OYMv7ZJS",
        "EXIRWyKWRhRdTNhi#w83@9VNEJ*-ENccH_Wh3D-VYzTBXJDG8w1fSazOYL3aNPcH=c1OJZL+0PXFLZUcy=DkPsdgdm8T4SLl6C*b",
        "F890eTbRr8cNC-kvEEIkUvfc_d*gqU4l6eTmS^XBhtoF*JmZJv5BdDg5Q0CBxC2&EpdP2BgE4owMgY+5wlGxKwzEMbmconsvHQS2",
        "F=~+NTuxHHfcWuhpJR=^+pgU~wzqzxWLK3@piUtmhvgOEc+McvGG+sqY7eD~Wz^iRBAdFS&STL+Agh-JNN3wgngG_z#-wWSgKLxP",
        "FPn2REBY8f9yPz*fvpfvq_A*pafex*YL1Wh@Si@w@bXC+x-fNgYCZdqZGHaYds0UZZVbQPEhTFr&Ck+JJ2v_8^Zqfbhyl-~b7&h_",
        "FfzgHJdLKfb9L6WGIUQpKYylpQ=xHrrwQ=_vOpc@FylS=c1hL1-^r+-7L=*-+f0NIY#k#6Uccb03#3atpJp4I9DyH5IlJwzZPpgf",
        "Fl32uzSTgB-HLSqa03rWyd*@NcQP^*zaMl2d~WJbTBfVbbZaYIrtc1Cdz#bK89pV-z#qENNt1x=hLu@OzuR@GomU5~FWg&BQhKQm",
        "Fl~nvUYB&Ww2Z2VCac@WU^@TB2WqNoiYw5lH3Lwgd-ERrMFyKxw=14UE#LzWLt9VuOrfgHg+*Jn+pZ#ycWWMxg4ZD-3d5Q_O8y=D",
        "Fmo9OYcY-gR3cdWto&xaBNUW1&2H-&*f5k1u&aNLeBSm&qv~8TZZmDMWqknlkwlvnul^Qvg7Ik8pvCdwYTEIg3ucTH02DtlXpW+l",
        "Fp3gdz+noH8&vMgXb^#c7~#xN^m_AO7vP^8n0CyMOIQ+7TgFc=Qng6AGAPunKnx2lm@u#4budqX7ke8wrJR9ROm*WW_6x2~gxwly",
        "G#Vte&Ks0xAo58gtBqR@83^g@b59zf@@DU^bPhBeptHSb&#mRLn8@PZpJ8=nL~Y^&9p~8DHtWf86pm~rgZZ&9U3whqhkQMdtOk^2",
        "G39kIaNy1k6Gd2NcY8zr4#+-uong=ntn27N@07cbv67*^VcJs8iqmhbOOFDAZkguud8-yeDrPg6BHF0OyR+eQ=ss8xOA^JY03SdE",
        "G9R^rwRL9-oib_ddzr82h*viqd*Vawoywt_BlNbsB0XsTIglZo2t*tNcu8ZbwnQFoSR9S364unXH8S=XQQrBe7W#COo_A*Gpgq+h",
        "GMhn0U4Nrvi-oMaFlpklhk_6Q9^frzb+CS1ffx_HcHD&B0Z-2ZiPgRN*R~y9_S#7KPOHKhXIsi2Ax1#6zK@96zwE~uiCF46q#O4m",
        "GRku#pK2@x+ml-bM~AM1K9Kmm8_17A0OaeeBQ-Xf3-*^MMT6sv8QgI0G~XAzWB3Xqt#hdMM5kdu_i@gaKB4b+F_u#EyG+Kbr-&=2",
        "GV*G=XqZMesnWOm^&-EJYdlfA53ywwpSrmBz@oWZKHAMaL==-l00eDTv4iqQX7Rz&2HB+gvpWd03+Mg44Xmx0@vE8V-R3UFrA2z_",
        "GsqssAp7nwhmN5E9p32xH-8Pf4fqfdz5tAB~HU2NQOgcU46g96mXKFyO~E+wAE522@9gw5EVqP&In==hxBrc^h5fT9JSToiQM#_l",
        "Gw~~0e-LPDZ0_HiIPM^6rKz*OP_pH2m6wD5-00iOW-h^m47q6^4IOWJzGPUg=@407~TfmE=hXZbbgo^BIn9Bm7RwNg~5Wyt@ORA-",
        "GxYtT*o-eaoFnfGS~*cPHsg98xGmaCaC^LDzl9@LY@Y5t*@_^4VRy7#0pQi-YLM-=y#hKHX0ksBX9dXdWsOID~d3*u_fNQhP#vDg",
        "H8Z+t-c2YPvrmLSTTtq1LXb80~o34gghmaw_NAhP*c2o~qu_~_LPdv&eQ*XhFS18JXOZx8kgzRYpsifAs10s@PsyDFH^FLa8zI^#",
        "HL1O~nxBrYkNL0@WWqE8fiduc3NueNP9#1g7z=^r612Masuvr*V~1iQ67sTeX@89b+D~4gNpKgN-Nq8Y-3gaiOcPaTpE4kOgGQgM",
        "HMzyunrr1hQ=@S24NTmK~l1xgMFQIDge_C5fRI^4UzDiuVQ*df*~XR^vSBs-yq1fgPKcRPJn3zqbrX7rQd=1W9L@K*-42l+1OtDI",
        "HQMhYSg+TSs98uxYaNR1*_JF+tYTggsP6srT2iue+44#vcOVTMfgWkV^FqDpfXeg9qmsM=&ciggpYgNLB+prxqI1ggb8HFM8OQ@0",
        "Hgcm4nX_x#DO17Fwt0bV@4duJ-bTEtxX-2-#VxAe5nf7XGlULHn6@o8J9ohuhBc6i4mb&_Ga8CHvS3UDN2#c5x2S7tpyHHOcr2Sa",
        "Hw&y33a+novgAL4QB@7Qi^ma+r4PQMp82XKuLApBNulI#2Ftu&&suKxad#gR2GD^foav4zXHZ~oF=whC^g~YQvhgdVBwLTlwtxcE",
        "I-g#HPD_UH#eg889kPCW44dT8_GFTk4&0aq@zhzAOZyyStygzbygEfUl7VoZIDam3XIUs_VX~bkLf50B~*fi2*yn6~Ggvbd_-2I#",
        "I2fusOlfgQ37KHvruArAt=8IJ4KWPQIYf6veJP7x801l#K209k+tmUouVYCBHg#OmO11TQ@xuGUgF-X-FD7UseorspK9^iIH5OHt",
        "IdaPIQHvZz0dPtl@qy&Uhl5*4L@vAQzxOB6v*pGNS+X@h^HtDve4g9Ee+8-oBY&Sr@5BB0OoGNaLW_BQJx3YHYtmEp98Rr-aNcMx",
        "If&Z*g8R_z2tYQ2bv#V@DPWR1vr*^^pNkB6_XVz*Rk8A_3HndKDtHIDVH9wpIyZoUhO#QESoy6Expk31YTu*^TZflq31vAPyzZwJ",
        "Ikg+LH+At3^9@#Hxgggb-BP+hw&v3~JRD5DExh0s@HrVO9Zxysz66Oy7ld4^BX18XrMOcOh&eSP@fdQQ53@UA4boFOYpylLJLTg#",
        "Iu~SutJW8MKT7d^Mq8x2OuADi_g8xqVgi*7_&vI_M+800RR-VtA9d7WRDdLURY^H98Y@sg5gzqKwEgrge=EAtKgQM_o9aV^e~D6v",
        "J2UgLN-csz@W9Vxo2t@H6oiZ0@NoTOwkyRi-PJKuJvyhG_Wi6ar~@PNXvg4mFhy~cvP*scVDpSg7U&@lWaI85T3JZla^T9hRQrV6",
        "J6fR&kMf_H^=qamsmyP@OGs5prvlWU#Yw~0qqVncaJ7&_zqxVqv+L#rYs4=oq*dgV0fwyiwQ0P*livb2fOU9^VONgqsTH_-qJB_#",
        "JFPB3gv4NYHtPZAF=N9CyMMX0VFHBfh=vHrP8SWz31pC=vV@ciJDaoG92Sv*QkysJ7Xg0Gmecf61R=NddryZe#2tDP+l4d4-Y~Dm",
        "JZ-5qDqKOoGx_uz&fQ0smExTevv7zKJPRH79=py3_o87e97gp@NN6DYB#DuUpf+DtNAUQDG~lOaNKKcJ@QBBLB^mOo&srM16mTLh",
        "JbkKLFD+VvZUhN78wGDmz=PV=&xdGlyUy+i#ZcUgY+xDXD2Z&0Yb2bAav@12*haDP1xNu1DamDPr&MD6ywgL_m-2DV6~7eFx=gA0",
        "KTy8SHsG0UZ2t#9n^h#U4UOqiNQ2D09XJNTCzs~Wk3FP90^#YyUKQLXVcMaZqchGLuEZrYlkDM-ePghEP37=21cz+sBJzyv31tkg",
        "K_iOp7BPBzFuogGyVH~PYC0lVdcCcghSqEWl217eW*YMNVFqu1ywuq^l=M2lsr5KD2r~NQYpL9sg9bVgTxg6FB~HZnO=mSP*KEY=",
        "KeU7^eOtdqhnX033EaK9SSMXR5Tfe_p@5F_bF+Je-kflWUK86hfNa=C2wnyYlY#r^3qP&0yvRFD7n^B~7hVgnHDXsuZa9I#c^ukK",
        "KkREu036Bf0W*F*AgqGQgRM2zHy6Qlg+BSse#NGMJhgBx&VBR*lf~&sNcbyPx*IGvvMOhe-T_XG=IR0XBYlEbO@*A6olKTtV=GLD",
        "KmagCrKZO=izQi7DU+u1ZOe3IOxSras4NlLiar3^0E4h4+w8kdX+#fQY4wuAkuhxmgS9&Oa3Sa@OfvAZJe9ft8Kqsn96&pK^oY2U",
        "L@ZVM3LlDo3f98#4xRl3~ao#Zhx3ZKMCJ5^dQ=97wz@rgH7D#c6rJiEe@E4rZ+WKE26Dxc8#YpFlMFob*azmRP@IkuSf4wIrE#eX",
        "LK#sxl5W5RJVAm&CzT4zB-vVNq2ot_P-oeBS5E&ILIKOgo~r@GM#ZdMgpdNDs&DfLz^Sl0gKKpxLgnRBWGTbh4oPT@s@=P=NVglP",
        "Lag#pLoduX5f7v+Z_=+tKFeDXd6v^@fT4v#^&Y*+qX@&Ni_s2bG-dU9G3QceA52uo@xatrmw3MkdZCC6O*^6wN9~5^AZQWLcu^=R",
        "LnSU-pczNSZ4V1yclTwWqhJ2kxNddWTTLxMG89_uLKg+#2p#wX~#HA_oB4dEg04@zJ12SnJ=&Q_uRpD5KmRy*dZSOqK4eKZZAg2S",
        "NhFrM9rk-xu7HH0K8yxGb&4pe+CTqY4s1p6kbXtrGmJWyg6bPSZdM&+^g*8Ft6egs^gOgFPr=ncs#YoCJDyqSEJ^=NCXnFevR^@K",
        "Nq*-gAUw3LUWnRvmh&ERHg3JvY4*iGkqXUD2HJ@CKm&i8~sP5GyAyXkSGt9qq0lssgK-b9uU8Box=TUM&E&hhyrJhBhC+HHg&9HJ",
        "O4zF_MUz^7iDN5gng^4DLy=6sa6i+^nAzb6ogJqh#&mkicY4XB&PnBA085wq0=CTbfzdPtIspwW58W~b&bVhtpSSGmudPSIsC7_q",
        "PHi#LV*LHbfSYMIkI0V_zp~AN34p3e6JBX6JVqwfRIFBa^B8_HMk3HflKV2#2E&alg5B^#44u7uIgJnl&NE1NnnIX9Lr+9yA#5hM",
        "PWvhI-vg*0ii3bsuBcFFG1gPTpFp~6tNSlzdteISJe_-ViBt==lZYc1UOIBnyeMc0O32^W@YtLW^o0usACzn+Rx=*R=G=t-frw4@",
        "Ppu+qkeY&HOUN^A4glM1UwEdt7sLRYu#7VgHzTRH#6h*TBfMlSxAk*AZC_79Hxm=LB1TbdQkpT6=W#Fgq47012W+ZUsmw@zM1mUZ",
        "PqGHgvs42XMGOyKQVgWPmh&Wgxky134WL8stNOUHznc7XMz77s~0wkz3gD=Js#7FMlVoaGfLgBY&iFg+=N*21fhs1y@OsGsfG32h",
        "Pz2YLM9-*=PCmsnHl3QWe@H9Xcsoecgz75Yn@pwWnJvmAK9&BV@b2dYYvVQZ3+MH^VMPVax28-01+ttSl-o#q-UT7P6Yu0OPI*0o",
        "Q-CsC5efZ+JG3G^X6G*QlCg4JrQQ^g@y@gOTF#^Yel+@yCoeEk96vI7*fuBP#Elpf=JOJCgX0FqU6xptIYV1L#PrPHwRUo3zPEa+",
        "Q0PFogKHTMtDCE^lAUikX72PrfGsLWClpYWnGopuNusn=gn4fR=AXrNd9gTS45Y5YIQa7gJx=^_M3*~qum2Nd5Is_JftX71_gGq+",
        "Q9qE2g0AWC~XZ8ocdPCkvR_ZZP3amuASy@Bp=E*5+^Qy7x4LsXLyBGQTk8XC8wg1bk7eSgcgSFGQt2MJqg6ei3*V1z0AwI#JGyhf",
        "QM*6WiG_Fi-La5gmiaFK5h9G4=TKpA5zfRAkVMXZd&f-*EutETL=3ul1e7+cCv_qdmd-mMW~eSh9R-UcSo=4Ctrg+eu0Hfs0pmH*",
        "Qhr9qXgineFxOCeXDCrBS#aXsSpPK-B2^hlKoz8=V@FfF^xey-v2KSmD9yOX=^QXZf*0P+@WksB5c7I6M8r8rpoXw&*~O9HDgl_0",
        "R5OIFuGpy-0D-aQnbO12KDf-K#wVG+VbhSJyGpZ=p3+FPhOyB5JM&ONAMyBRyEMIGLR5xm7-V+I8BqF22PI~a4z=kO_6EJ0A8V9V",
        "RPMgP2AN~WNa^JAfNgxkHrY=ZA~bER8FsG0Q0Qaywch_nPP1biYoL5BoXpg2iwTyFd6qds6i2w#Bvhyu5As9Pd8yRCH@Oe&u+wtf",
        "RkA*6prsd@Clri0ez^Z*Yty0AqhXxgahQlLZsGgs59kQQkAq&*de7s#TIhD2A_D9NBHPYhwir=8Hc2Mxdb~-4vU7Fl&boi~Zou#p",
        "RrxZXug~tB~VJDf42hx6cWFG+At5a7QRGMg#nCKObu3fWb3BQ7dg05d5pu-svwzbr^@V1BWe&gmR3i*KbZmB~w*hgrF^g9*lQM_1",
        "S-VT3CNMx&^7No_iOQ5UhggdFgA1OfDpu6*_VGkB6RCZh#zTVUEpdlqZmcd^sqR8XNzN*7BPVkoELm+GqKyJyr0~V+n=KhDN-b6E",
        "SWG74x^kHLL7n7N=lhRm70IXk12elq786vt6KNp*-JvSs&RInhmg2o*sugTQFpC+NVbO0Vz=iBQ=vD=25Wr+M4p~v2=xNU+NtN10",
        "S_4TsDyfqlaBpLP6QN9NHqCDgLN=KEAz86P7+LzyMYlcBiozV~GiDmc8&l@UBd3ggvkw5l*AgfrtP&emGapvi^mcrE&ylKuRdsgr",
        "SexpgdzyOIx0hdBPL&Y5WO+ehGwAiR2_EQCE=bM_KcrGmEP9~-CvZNMUEpqlTGQNdgR&9n&5G3J+pfGE+*FQN+Qs0+NBI+g-P3=9",
        "T&l6of49MBWS#0TEis^DM03uzdKEGRyaguSHdvl15ZOAPtwGxt#=i31_mfbif&scAyPPaxtOFVB7oWVb^Hq_=YeWV=i=BUELBZAv",
        "T2Fu819qJPz+oqxDVhdGzzc89=#5sim+hqSqturp6ZrLn0#W9&~mU_idpIsv61bdh7kRTAkK6a~8b1vcLZPU+gNr8l0x0_X#~OHs",
        "TKrJWbr+LU0skO*yuKl=db6x^ug7S-wDa9lY@W^LTrsboKz3hkhaki_+CRg6~1kM_yZDiCp7yF9E++Hn2bCraQZeneurGQ6Ez72e",
        "To-oD-a8zp4LaVn@o*FBsRrwyMD~&keYAzwc9yBdNVbXtscq=y+^HsN9LCurs4-_W4vB^a&KHogBTRvTBe12p_m_#fTRq2VZRllw",
        "U=gPRWdp1CgYv-SV0T-Kn*gYv8=CrsvPi~&1mi7R-LO5IDoWKDC3M=b_pm=es68Y^zldQp5-_*R@ReOLRKJ64SB79KUSKst6AsDf",
        "UAFOJ4k8ixeLCZeKQ~*&*2N-7&EmVKUJE*HJKagRpqtXSipc&fTIiDaM9e=wUtg#LCprVmliD*fOCpoTwnSqKg0CuGTJtM6Feoo#",
        "UB5V78yrotYK8qBwD5*8l=Xp^7vgZLcYRYQ=#3Vyh3-W0GX6k2FY1M^QTOGAY#4gYP_SU-bGc5+CphO1Hvp1qVGMc4i++8ZPFsMB",
        "UKxJYBF*DZLO&4QUGNJ2uWXsOJY-omVba#Ys-YOzKfA71m+ayRRF@I9Lu8Ec99kB_RVze3RYMMoXGU2d8wqV_f~50@-F@s1VdxQ7",
        "UOhL3uP-=OMDBnJ0N6g~Yomuwym+L0lJ^z_ifcP4r*mztvto6xmI==W@Y1UM-k16wx419-O4i7JWOKoig+72z-ubx5GlOaFhl2be",
        "UQ9kF*mIcs*86elge38@r6@Y0QRQ&gHzUpVC1Pkw17N^rB+xUNU&-MCWqk*VEwl1zkAzyVWHT=6h6To+biO9b_NVIeSscC2V&_Fs",
        "UU4AsJ+2FIr^L7Uo58m5oqRmvPiwkw4b3nKfIq9WJ9ZCRPqP&60RTENA~augnkXe_kCiG60ggqPlm6=V@OO3@UcWd+dsshJCkhBZ",
        "Uh06NaiDnaYwSk1zduk5=34C8HfkE2fiBxQpic5fdXMn1=lRTa3EAE_mtwboMSTAhZ6gi8r8nqk6eQB2V-MT=r7TeOcUUBwBCHGA",
        "UmQv-agclacPl~@UOwUpOLm#3@~955Ly5PHC386_zvqGtsLkbwl3_4qzsZP2dGA4pCW9KzpypGd^NvshGPmSRTfIgQn#hb9kg6mM",
        "Un9g@@HzXi~lYNR1~P7IzV8ZGpcoX8f4Jzc5PKJ2Q3mgFGCXNy0Dde6ZzCZvWIzSx3rnK@z3fR33Da8gV@kuWWHUaU0F10ViQSi~",
        "Uw9FJ57rib*4IelsEP-hSKIgi_bJ3T-2gnlRkDB*2+xZQODIHJR5h^ga*oBd6b7RagJnzSt2beL3aexsa=^bZK^7-7pkQGAq^~pT",
        "V*giviaWhpr1Z*B6APOk+qKaPoByuP+R3Ur@WWZooJBr2csHe~GClCotu-y5sTk6L++-Zc5b@uQ7L88u5bYRwIFcT&MGs0c^k=qy",
        "V17~rk^wYhK_k4Ag&x5KC=8r5uohG*7B&utRflU2SmqHa#*lb&Bk0&_S~OXJEEYgGYRV6t^tilBoNr5av7*LrvRoCCHS+oXwAOfQ",
        "VAB&nYCV#hEDcm8zcqKam6vGc3yezMWzwE@o1Bbtb*NdB8gbJH8nEL2nF5Ss-6AoDGiGt@R^65x7IhRvZh08m7F=Ke0gxsDnW19R",
        "VKY9p+tV=eL0fRN4V=sw_a3I4Odi@+gghKp2o+GvV*6S1AS9T&7cDBDsTCk1HzK96OwxTSuK9LquSW-w4JxJ^OM&Jr1wBwl4dZfM",
        "Vdz5Ut*DdTt~C4uNnbuKt#ZR7-vXub1#31~piq0=86zkGJLX3f__Ecdfudi*4tXq5okSX9&I+1pn^A7A5ht~**yfI8FS+g_mFqE~",
        "W=yFQ9EZ#M0g~QOU&4Ub3OkHMI&Zp#FTFqSKXy~IBDPvt21c4Jn-RVqLCJaT5dyOqG@gFhL-27brU-4+Ws&EIC3fODxysN3Jqc1P",
        "WNWYiD5xeSl&g0~KlTBC-Iz2QQV^Rs^okuB49EZ4k=u1+XqTWpMscQnNNc4+BO9Y8vWBcAr6JwLiTT*FpLwEI+1sKG+@QlhmEive",
        "Witig8TUV3g3Z-bW#buJbhER*L4f1@hg1bk#gnxmxDOpn9ttgI7LXD=RgPdPn2NleJ4fqpSU=ByK4gDiBi1KncNVG~7pTCDl*kfa",
        "XAFWrcHryxG6uT3WoHz=@t4LhVAwJzwePgX1oFrqVeclbVF2@Pk@PpMfu#aW^B*1y5RIUhS#n3f*S1a^wY~tbg_-NyQdBA2ha8Dq",
        "XniqVD_Uhe_oDzkCKBRB@Dtgd=n1YRe33yEOa#ghU8osO30cu9_mzDgZo2C1tEFc2DT5ASdd7i_EtVxeC=BCsEpz=yJcM92aM+RE",
        "Y3APOrX61t6ytt01t6qfA7zxd6z40mlq5#gzLlEBvAheMki&JADcpdK=U0LT9u87dHfgA5WigWbT3eeI0u3a3hIdOS1wkydUUA36",
        "Y7C8ZUtbp0bTKagz6NBTr#e^r~#Y=&P6kYcNHZXF7vR*A0f-YwHw^gC7CGV&BWLrG~VyB0@OvuDYYFtC-bDB*LudRg&MwOSdk5p0",
        "Y9C+Y6*&Qpa-AE+g_gvtreN&~vyDgpoR^wk@p~RqAq4O*_58+3p2=sZwc-PyL^J-DY9qbutoWBN+h1rD^c1mieM0ZJd_q4LXJv6X",
        "YGc2HY4ln-g~MSzOqyzB7irqRGXcTCQSIg*1=@s2uhb7^HYZRupN2w0PI7J349FJltFc+hMf6W=fXWPd=qvI5V1aO&2~nHH2zybf",
        "Z0xqBlyC^8tNbqH7d&F0CU0P*^vLgsylgwiOhJg7icF3T9XNgWtvKMTBMkF-4U0Ve5MMvPz&~cxS#BhN_w-@UufqWCv@rD~uo*a9",
        "ZK6BETGeXzei~KkBg&y2QLY8dPPP#Xhwn^S#BpK-=3S9Hggt9+g64oCh8JCatgKO25Una4AQVie5EmSgX9onZFJLMiLlXAztM7yg",
        "ZZIvkCD^d4vR2SIeBU&oqFz6UlK3PWR1^Xk8gQ4umYKZf#sH+MyF-~MJk85F&8VDQPd#+GAXnXCvgfOo0bfdKggn5_9D8xudNwKt",
        "^Bf~g91dXwkA&itcNvrbG~C#1L6vz3U0883ieg-7NiFedWt@WEbPc8WoipUbHMgf1^zou+zng38OaCD~UIGetdipWgJa2pMO_*es",
        "^EUIbVlTyo70q0ic&mOCDuwc-ca@1xURpXE2BlY-kJ&USm77+RJdwpDDe&kSC05^ANYYAvxvS07lF9oG&F7dHgnR7qJ8pW9X4GAR",
        "^FO_U+Un0qaTs+U1WlI78~Q0QpgvadFaX_mf8gZouGwxXqNJxDC^Z-Fa@O9S47IA7pxGYg-^x2hCuX0gACtO9XNcNilHW&wMdKYA",
        "^FsUms8q=fgFFy7UOc=WCtQp-byN5ccXJfDsS#s=c_ivY~~~3Muc2mAa8DcB#E*9wvKpBUF1+Loxpd1&gx50K@=LyZyKH=x&#N9v",
        "^u#gyPrpXgUrGneXqR2En3122eI3M*DgII7ReT59akRMOUOhHK*w9e2a4UYr5JPAPs_-gXGnNgTLy_A6y3tSb8CrfS2Y-2xprUSf",
        "_#8*S-lAiSrmY@Dr&RYVs+yoyAnMf+omDq+*2Ve@nAKnlBp1JBVugFN50_f4Gsllf1GM&O9avHmIG=q^hQR@Ga^-k*cq4fLC7xMl",
        "_^CgI136*0_hN-_=A=Hxg@A8GS&HcEiNge5a6gVCX&hLdHmEN5NMc7Iq@AefM3apm-gDileG6Y6cqy6&XIQSuyO1CtzBPn@#&hne",
        "_aSQOAbVQHk-s8I*N7et#gH1g6BW-^9w5OufM8wfD22r7mkGe_yhgOVAVM0o4b^vB5HrPz^ffZvQdMomi6gonnbQaqRAawQ-GIEB",
        "a*@mwypyW+dczUSX17ogkBC#aC0U1nAfXohg^f3PNRcv9U6^NK6NLfaMAEJuTML5AwweO~KP4JFcsTZWHXSS36Mc6Apum_8YmFz=",
        "a6ltJh@MOy+TkoC_HwVfdiS6PSCmE-Ne#^pP-E~ch+8WktvT@Fg#u@BzH+yJDIMq~aziHFhd4zygLbdLTV&1nSTGWZyHxKA^4ITa",
        "a8HiZ~B+AcJl1xWPAexEW3ARd+~ZOw~0LwIRaZEKBRWxY=MoxlZEBBhImAMb7V2AyJ5xBUfSw&VUUxQ76#iAqOx-gP8rHI_+gLbw",
        "aAhO&f^l5vUwxkRWQDVoZSZ*vBBCh_nySAyfWxMGZDWgJmEgLhLaeuJrOHfL19i~AgngKSUab7K4aGKKhH-G=vuRH@kPHCC=~KVy",
        "aT6OLgM&@A*4K~xn8oC7E=p16dnfxxNz#EUl9BW+2z=^z-EPKSEVXZD3XAcN08VhW-xqRO+Lc5p=JIsLuLgLft*FhgddHDy3Q*QO",
        "aUaNp8BB2M4Fg=tFEB6VNZdfsdcWX*gw4XK_B-9D4T6k4kI=v&K=V-YZ4JT0P+sW&s&ZaXtng@pi^iBlr4A_u4wfPQ9qfWTfJ7K3",
        "ag0@PF^diET=sdIfhWWcK_g~N4~ptqWX9Rx*cPRuLgn4SPvgK87DiJ~d#z1LcPEbOwWxn2qdMZ52+8GZqqLhGTMtfxGePL7aH=Lx",
        "avVy5XEGpkfl@W2=8Ggw5v8^^6*VfhSb7cHOkvTCf&=g1^IWYgFg2s3p#E6kXUGVCDW7yDGioniqK=O=&zgXnyzVsIwr8rtgD@@v",
        "b1Kchxi-g#g~@o1#wWZC_64GzukxL*lyqooqwv*mGio_B7gpSN0Nh-9bLazqx_fmgEHSy7BryoT+I8OrIJF~F78-F-4FQgWY7-Lr",
        "b5CgXY^_EXm7D+*=JeS30+gBrR1KrxepQO~bWK1L2M-K06ge+#XKy@#o9rd#--TNFuMsUNNmk0c9DM@EJyuRdxMxXO2*w4uyu2fK",
        "bKN0xpZAGV4IPQXt5g6YXcE_y-@wf1~oFE_+W0qi4KlpER*Tr2y*d3AIg+R5Uo1sHQ82LJ#woaWbsK8L4O8w3#MC6AqQP3*qrc+i",
        "buKV+48=4&fWgSoVGkl5uR4#9E-^plY46sCP2Tgwq59fpgkTK4sI&CV5+2vpZwWgTM1meYsWsmnI#RZ-izdGmaPSRJyFw4vBgeq2",
        "c6RayCkL9Hw+OXpnfA-dM8x-*&5Q-8tFxcoU-K7#I*~3+4Ypgk2U0BUb6nSx3+^vCIW#&cvRUJgdlxnO2tkyrLGoEkbsyFYdu&hJ",
        "cNEb@nz9_puKoOstL&-0uCs@IHS&Iuviw4VWPJwf~47X*eO+LwlrAZ^l=dVX-~E9b5HXG86RYpLeddPzbO0sAgPJ-Baro@BLsg+Q",
        "d5urgBoQlGsqKQQYV#1=7VT~MECIgM0nlDDD^0GbbI4z^cx3^5nL_dr0frPE@q2tklYC6-h_eB-hX#Pmgm#2f@XWTHvLumbMxpfc",
        "dDMVo-ZqtCKOh3KO4&ucX-K+o&NDKwP_@b2^eeglv-=3+K@yMprAqLGOudL5hGpyzkLUVJ~MY@axRYit@rAI5@vUUR=E#Zn7UtQv",
        "dJg-^vAeyqEg-p@wrCEsVY#KhldkiOiaeNUY^aJB#^P^tse2lgkoo1#Q^vsz2ntrf7VO-QL~21^V+_miSAr-CSmzgEY1Pue*oUfe",
        "dmGODDLsZgtJlXc_8gevr5CxC03@Yf9UDNtxbb1aJwNZP#8rOE2NGuXiGgfc=Z^~HeQl54GHno36OvTdUmOn1lkmf7YpEY+=0#*O",
        "e2NgqC2gM8B^I@-~oLFFax3LmYbIcS+egLxo6_bX&ifYAbT0h7-9iVfOA#*mA&o=&UCtEbMJU7+CD@+T6XIy4&uTBsT@ha6qnRka",
        "e8Pa6UiJuf&NAbPe^ZRGTnm&G#2eg-FllDxXo@s8CwI@BzD@83m+J4xw@iOk@nPgYU*+IPg~^=QDP6N=eLG_a0dgKgadg*8nRa~A",
        "eADT^+qNeMd9YQyB#2JN6o7FU4lx54z-G2wOmk3xFHg0x3tXf8vpZiVv_hcmzLvasOGCcx_FE5aPXZgEq~KGwJS9gsuG_5m^^+_C",
        "egodxu9peL&-V6czg4=X7HApKBwprCc963tsX9ilPgR^&bz&bQBz2HtxISs72SgDIcza^6i6*Q0^LWv2Bx4a=H~87bKzGSJneOlF",
        "eiI^TQ1+t0dHP9EgxWrG*V&rNus+3q~8Ef*&VYHwxnwHBz8M3ACN9VtwSdQ9gP2#z-w6ZaSfMA_2dvuvmfMVi16nDuh*p8M4np@u",
        "f59Udv&=D@wPyH&aFtp=v&DgwBz2FKAqVNV~D_4ym&z2UBNgIkM^tOnEL@8n#B7kVT8g_dhY8JX4#sObgespf*vk@AtLIzAVyTh-",
        "f7rkxIS#1maSSU@rES^YUsxKp5gG##3g@_eJkKuhKKNR-SU5md*7PUvPgaC@TLOgArgS7JuIYe^JQNWge+YlG6d1IrxnEaWk77Vd",
        "fLT6oYnf~da53G^V0mFnNvaIQIO99OP6DTM4Fgl-MnlAe1bHVym0yAvtgtgRPpQ0d+1~yRLiu=s2ZZqI=_VKGRMxIreh+Nu8y-UM",
        "fU~UYnsh@eAg&N+0K_cm#IcIfBCV5DeJO+Nn&wh+ol3ES4hKrVtyM4a~Fps=~cHY~q@~arlG5nA+MSImh~Cqv6grI5HP2YX@vZ_N",
        "fVmn2neAUzkEr58-B=n3O4flDBx4_-uf4M3LCE+5gfVKTIJ+03=hQ&Bv8@o_NmBhIo6K*zg6@R9y=4#OLNO@QgQHk^e#ZtUx9LrV",
        "g-tXcrBrG@#S5*5vVV*f37GWBuxsNmiq@RMu9og#cr_eugkYylp1=EuZ_B7g@nxQ@tJ8k1zhM6_x82fngC0WC4N+PwvWR*_~1NCa",
        "gAa1aEG7T67Nv6g@75ZCrwSg8NygYnp~4WY@56UhMx~B40ItmE5xrPZIWwFX4oav=ChVb_+GEXHKZ#yPi^CUQh#cqLY@eyAZs+De",
        "gChGDLkV=^*HsZPT4BDaAI7EFl*b8#ogRnZHLAXoUqaiVEopULoqvLq~1oUK~S-Zg^YYffCxd_+BdJt6JHK~F97QQr7FV4~&Niu-",
        "gZKRtlEPNzo#sdD~oclIgRKXMImqTMCga=DF4IS4r1xGcLnucfCy#I2B-wODgTxERZ=hePgS73zDTYJ-gszUGPasq^b6~=yrV~ZQ",
        "gdU50n&B^m^xXEakQ-JgyQ&g3pZ+n_XgAil7pC^le9~*bZvtv9as=JxN^xoBgUH1EG85-@HMww8NOSgMxD@F+CN5Maf5QxsVpogF",
        "ge2sn3A-g+PpatpUTCeR_qOT6P71FBqCvg&nH~DRy&Bicig^fbzOe_QVbeV_v~wABEzg2tXoDGIn-w=PKmk40#G5qrmM4lO6KW+N",
        "ggFnoDKZafE#LMbJiuCJe@UciAzRB59XrJI6^#oE5pSNNErwzt=BYw@J=O+gz8urBlUctX4Pu5g8aBqJD06ml5@MAth-8BKTm_==",
        "goqgqAagm4uSzJfPY@e4BVVHby@kwU476E#=l_q=B=Lo9M8mdW2&L1k~gidGzG^ZeS-uuxJch0=_4IGMclHgo3o4CFXlFc5y9=J5",
        "h-GhhpiDK8VTh0ecbxSGZgVkv^c^YXoL7#CsyC-GtfWEHg9o*cgisGUfdveV-IZhwMrm3gcAcM+=7DPls&@5z_0~uV~9n6kugrY@",
        "iMtX^747y1L=dkl34Ub8R=cz#zSY7tOXzgKNElY@TYBSyF*gOAX-4b^#O#SLtCRAGOxm+tKWDrg2uE+HV^rzQ388T7-*XfmhiUFf",
        "it*R=^uEvubAtrgVIZFWuvvntBB*z-27em0cnIrhUlYsdF=APUmE+nt3v82zKZQXVZHAc8e4K-J98iJKdoLFo4zHginf16BQ=htm",
        "kNJ4Tf3rdTB8A&OE0*8OA2@sXcUEc_71+2ZJQMq&Z^OhqWiMpZpWXrZKNC0Be-#wrS=G5tIiEEp9M0s^r1wqsqT6@RXbbvrv0&HK",
        "l6=TBTs~oXBQzW4atu3zg2gwgC~uq5feVYxVFB&7g3ZdaNI86p*5CmZ~4vpBEbCI+-8FFt=f1dI@8zWqPRIdTvDg+BOxw2mA9Egg",
        "lYvAm@b*K9^BEOLQDuQYfZ8L*zc_V4f2x-v3kPkf~ysL64dOXd-uKMf1x^7n#H++9psVSFTxim=YHokeIOd6SpUHlhWvN@@l&*@g",
        "l^EGf9q1zMwaFULDUR0wFRUs~2wfwRX93XQDGXVu60fD~GFGFf7vViEs~VeFul6#kdonYgcCs7Uwc9BGpb2Awq1M^WM@uX3oR0xu",
        "lntgeYQ=Tu^lNla71xffzet@DXXlCGo~r2pRl9M1#F3zr38foKP&bbz*gBk@5-UE-SEAuIX~gtMSwJaw=aH8eLYSMK_pxrGWsgrg",
        "m*DYz1b3f^nK0GokQOg6Cfs1cm5_oAz8OiloCKVGc8WxVAlXJ=hA8Y=gL_KF80SWerR0ewAVOU&lRd_BC&Bx1cz3Zf26IZI_9#fF",
        "m=PXo&^Yq5ext88-xsBX#iGcw6CrgsJ~+gNNnkqqPkBe~XPwlX+#iWWtPadGqxo+9A8nV1IkYTH21z67W5_TGgDl4wY=E9nIPHow",
        "mDw#&bIwRYVL83MgIssEporrY*PFiAPNH^=9MKC3+ev7SYGr2bAdz7T6a69NcEp50o8bA6E#ZL~=lz1yDKp8hwAvxHY#NLlgxPBZ",
        "mW#zr#@X9@Nhy=5Blx-sb55~6T*ty4JcK^amucl=#g#bpHZdxr@vZaDY5gBt-IN^mlTTYcS@cfswdD+6H~3U9Ckgwq*QdiJL4pbd",
        "mmQ23i#Jln9-bC5AOo9mzWY5Wc1sWfG_wSuMugyraR0P_V^rqffZGra62+EIE#cnIiT5c=*e+GNqJ#vTuaZYKYv3wgbpy=nY5d^Q",
        "ms6kIQToKVu4+A5bMuc4qT1Fp@Wpv*-NHdxAB9lUnIt4*9fNfqc578r-zpq9NBg34m9C*+Y-yxDm~GCVY=vRZxEnPmHpRd_kZ9rh",
        "m~C1t*NUkn#SugS@O33i-zI7b^=tw4Sbcikg58Sq6t4cQkODSTI3GMkszmg4Z9wLZ39ha@ERT8#5zu6*+sFu++B*d*=I6nCfJoHJ",
        "n#VRcZ*cQC#4&c6OanWK-LWygWTmXVRc#bhpoUwFMwMJxg87hGdbae1isP+9*g2NP@6y^WgX9SuNnTZOgu^6magc3hngcHqzID5U",
        "nRZx8^Df5mTVyg4^4@*^i9MPu*~n+Ac4XwA3#gS8#x6h6KGg5UUS~gY#Sv_1Zy34Kvr5Nf3Ah_l^qWb9sv0sxZfAbV#^Tl17~9a&",
        "nbN_FJplLP#6ANBv&wiTEndH@De0fgolq#vby0S~QSKgI3@HquW2WJ9e#mviS-8h#fk@_wylKok05GFRlDT=CHx~#YK~w8qK^I9F",
        "nggGq-r5~*dR0zgMiIJKJ@3h7A_b4XFvlF8K7G~iH8xxiiDHAg8*g_GbgYavIg1gfxqoOscn-y#E3CqiRxvqfgg#2SBsoJ8+G=l+",
        "nu^@WI=^rhX7YCzhGFJr^wCmyA4=w1~88mF4W5kzFgM@D*i7rX2pq2ZRc13tr#aGgII@0671ZD7lU0S9hR4iYZg=#64DtFJ*NWWS",
        "nyIatO49aFv+2-^az1QfZJp#cIb^PPehbuirx7GB#hre4wCDVBW~WkXAMH&JOBafBbW=f==u3-A^3IJlS_L8A&WqLRGhV6mi@XNM",
        "o&Xi&lFYik7ywHIrR1r~n0mv_7yR6wdwOB4ZiUhyFRdlhdPRf--OHRa0^LA02XYgn4Y88~9JGF2~N~B3c0PJR@^8*AY&9IhY8K_O",
        "o54r7I34-uO^wk1oI1=*BgByQ90Zv4c@30Xb*PCMI2ZL8^dQ9dhTZGiEz=1hu79YFSAekKFgGqlQxfk3g*-wm~ZgP0TA3_WyOH-*",
        "o5sJ9=kedm^XkRiH3Kxr_gI@E7+#Oyyv5D2EF4bQM6~29xYJQd7e5gdLLTOrmIXVJedzImdOsKx*rYWDl&I_bIqc0Qx*qE-w0t=l",
        "o9s=g&_9#7OhB7HVf9DLYyq^mrplLYLvq9rHyEw_hg33dchUVr#2n^IGe88iAdA7t9G~xBtf9YZ@OTtoMgCGlTpGWhXOsJo20==G",
        "oLkobr=DA3c@RZPvIdgc5YlAm@0yFL^Tt7K_f*Nk5gEWDrXFKm-bzN5GpI78DYHLu+yfR1iyxUYEuS8K9rhVzfCLU4MaUXdYG@ud",
        "ofMooVP_f=@ShcyrLgASrgHQ97cp7Seb_VNo94-nB3P1mQM*BgsPM_D5PV-NM8fpsCB7GTR_vFnfVgezBV=RN^^zbVn=yV~E2tmo",
        "ogUxPLXbFvW18eN#*GgM7iyS0R*UWAGzAmggrowCh#AMp86+CVGsqsWIglPngifSU1WvV@KlLyB2O0IRWROkSZQM-V6ae^~ENNO8",
        "osfaTLY2OqJORSLn^1VB8Z4@n_4e=43Ug2ygedQPoHg=9~*Jc1NZgpgDr=dFDIV6RIp_JGBFRx0hM*SBeQGCA7qr#6Hu=##qsd=g",
        "pYi2_7ZuR#oH-#MwLm4&+O~gV1h3rd+Xomz@k@rOb2*Z#~it^XOUwaguqN&G6VxxMUH3Ha7l*2fzNoXpqRNhMMuxUJaQ4dAHd2be",
        "pbZw_QBhyg98WFgL~3Z1dk-d96Hv29y6@Z9*ANXmLlKBe_s7gRghsUpJgVJSwm@4n0I#g223YKdBPJk+sWSeE=Y#V9o71u-gf7dF",
        "poRF464qRnRFwfGxkYmF*@kkRG4W+de=gQ~huB4KUiYex1mRgGB9gqym*ocG+zgcGQXEJYNwnihilFLMFi=xsJ__vnPA-8SsuCvr",
        "q=mZxum5e#fL7nBk11ybBtuO^8dRKYMtu@@+mYEIg^ZWAw##y6ohz=tqeKVASGvWUoALtIvHlCdd^g9-RqWLfs0H@@iyyGC8xgde",
        "qCJDun3VKLAp~YQYvtEg~*35+yEGwru-cbJuQ-e2I82*_Bspb3W2qaw@pHnQ1wCyzK*JU-xiz-Y@X26EdFZ4l4gtQcgr^uU45o6M",
        "rE-10yDcx*FX3FMT&x&eN1IRaJS^BBa0&xsRw9kNhr3v~xPq=G-=2eAQ3z~v2lZ2x9G6Z*e&Xge*WPnsttw#OPT5eyL_7G&cefLH",
        "s&6YYQ6=BoR*3gSDoaZ@ZglktBtc=d2B#=H*fb#cbG=lxH@6FR@=w8qkt2yQXFr1xgkHhVXfa~J3+dwwwXgEH8NER32Cpz9u^n7&",
        "s&FS#HFAX+gz0zFGZ8J0k1wE^CFgv4k4Kw_p1kzDh-D0X^V&@*gLpzXiGC6kugY_tkKu-nLbNHK3*eB~tV1y0B@3USKvlUakB#TU",
        "sEl6t&yzlkyINQqcT@0q=&9GIlFnZLVO3v5#+g1v7~1wpb-QJF5e@LO6RcpCge-2qz8rXkwFMo^JOt7wwmYVHeQ_HDVoEoa*~wM4",
        "s_zJSNmzg1GaVcUss1-@_K+Ac3cQ5EuYBk_R=#UgpUTVJDksCm69Nv5&74gpV*Uc4f-nN7qOwRUaOsv&08^EKBhsb*5b3SIY0*CI",
        "tGGh*vC0NWVK4BEnEm7C0lO#P~+T~gTTbAzyOq0nM7GWZ7vXQFtaiK-EixEDJgmQ2COWtQWk&X2*F=iegKt~C**K9g++99HT*iFR",
        "tL1FXaYH18A1hw1P3apa^8SCI3t+#~&S~BhIzam^MTy96TNUUtf441&m59niUdP-hGQXR1_+TND@fgaEl+6I+yf+lcW^F@8R3kxC",
        "tR6R3u9gqNo#Ul&eYGCb&D=R@bOnE=#VcU^VJYWzH2UFlCupCr3i~LJCliMwr+iIOi9vAOOd@qLTLM3JrVZ2_z_NVW=5#RgHqPMg",
        "tXOIw0EA-ThfIN#Z8DZQ~~AtZYhsPumheyLR5Im@7Y&PXPKQtO@9RZ-P9h1~6Ubso=DIVpg5h8c9&xknCU-UPNaWJF8B43~s@y~M",
        "tncMtcc6wc7CNUXG@W@MZ^yUe6rDLlou6Ov4ZdULMurq8Ga7rWbiBqnL^xMf^D4bq&bfPfrTNEBwSHiEv1a1zrivaZI2DfbyG4Lo",
        "tv-kZms+ie0SH*0hYi018ArmxhoDu7Xc@MnGNFpv6HTJr#AGTgNxBRlIVoOCe9=gxTW9evOoYOsvnXI&bbUgKt=64gW82yncPscq",
        "u0Ic&iC+IAYGlwzkqv^c8kpEll6#mIaU5cm=B~71=s^@bVwVnS88vWD^3Y&yBSaZXMgcgD96NtB5UtrOB~6FoLJl2ut=gEyCshOI",
        "u0V_gX8tTvS8GdtdDE+1AL*k-fY1UZk8SYBW1=Oy6*uYPvBgN=_J4vG@Ju1IWM8zHtvK_sCXzbaflCrFUvecXJuKSIer4R^KTR6y",
        "u76Uc1egGe-NL7i&yX*kw#4-^x^-KAOIA7v+iFWw^g@6zo&#S28CZ7nTp@V*C_ia-v=daBCk^UJJZGcKrg41glGVXh~Iql=d~kHt",
        "uQKsQ-cwL56yAP_4Gb0YdM6lhpV9U08c5rJOfBqD36kNzQE==93kGNUg&-=6DibGwoGlntxdi0LGZBmb^2lkLar-rPluE5GX9u0a",
        "uR=wIYgtlgSKxc9HmaCCBnoDN=eqChUfOIOHqR~=-L&cC9vk0n~V0aUq3EDp0C09UF3BP6a0apriEZc@2oRd#eC&AG-w=Z-ABk^l",
        "v&rf=nkplBFZkH4tgmOiJ+afrqUorBGTBI-QkF9I-3dMJNSR4NTKAEohQ2g-3_gtY_tPyE_lSrm-F~3E2ZE=EXDHRfDqAP+Uy&2T",
        "vDBOAcB3XvGKxitFMdVwk@SFUoM0m-RTTr1zF5=kzM-DmRmvlaIk5Cg3&Mlkz@#8pd73a@2-IZsYIOV#ol74~gg4mgwn=RmNSsXT",
        "vY=UzCVRnR@w&_fHVPlh+-t5E*JeK=rn3UFsghpMNS&lyO@H&RTx_qaYfG2gVx3oPiWNq&M~gKc#k7q8A7W00=LoGy+#ovGa^CkD",
        "vhZHsBkzT9rTyF4p-W5@n*QnJTobqL7yfvG&AcaH4i_p9vHefmMz2Lqg4thSE4H@vgTxFCVYrr=OCa2XgIgBJXaWbeafGTeUe#eo",
        "w2o@4PLILa_&Z@=Yuq80fS+CsqrdNKOVImqXhkg0*G0GmdfZs&uRC9m0y_S=d3Cb@aZOoQ-YgorgE4vDoy+fvcXlyl+QL^_dS4vt",
        "w5dGJ9@LxoDSQE-Yc2SXx1K8K41Emb=yVcs_o#IzX-M7zteY9rDZKXa+wPk8fZab~myCAZtQ&q6psvcfEfBqnC6bJyLf7aL&yDTk",
        "w5vXRRLgvTaBSUg_HP8T6lwPDvGfkxrVeG_DpmsGSctuSeq&4KSlKNXsHLuh0GdcFDYL&L-SOg3QwXQVIi096p0gNytribcrxMrg",
        "w9hcoGHUIv9g&dfagK8AQ0kYGmT7tdXLPx~2=U&M=CgK*aPPuk~AGL70vWm5hfPfO7E=hQ3u~5gbE5Tlmo-hn*2pcT0*ag=mBS1B",
        "wWU9p32xB92sgS7gAba~0aH7P=gKQrXpV&Ol1cJ_PTAoVM3KhezRfUzlG~ItaZ6c=yh1alEZ~ETIx~-r@WNHHmIbZFQ0SU@T2OBg",
        "wXQM1++NUzIz3zy2@6eC^15eLSPTlkC4XE@TqnPqAsATDUW=~pgF#Va2*rP27+IgCBdyBlWNEBK2SBsGTMCthg36z&mYq5gLpUgz",
        "w^VpBgwzs^L#0=_l45DotluWynS7JldNUEUkKdW=P40A6kgDqrpmA~kWwQGy+hrQ_QwsPe7uCaD7uDW2xHxQaR3f&O9&nm4poDgm",
        "w^~q=hxug7~A_c3L3XUAcfgXTSOAfbE#dW=Hpc84qXCZO1dLbRGqSF9XNnRe+~@D&n539R848tsnf_I4EuDFWKIg5dHLJ+Pb-S69",
        "wm*Y07&KHQ^d9HCxVLXwLipA@wJ1h#Up87F~HTYFnLWvqZUTuYymq_YdIn5qwLyAna+tNQ3BeIXByiSgZ##M&q-2HX@NepXG&F76",
        "wqGdWvyqpqHLqH0#aCOIyhrLgnMlGs0_FqD-^igVf+==*fQzx9=A6l&THnd~wgFxSzBrBdgMBq4P~l+NKg~thl_ggcq9_NWmF2kA",
        "wztsC+&WF^Ds~~Bo5oqgw~3*WAftfNDm#8Ssfd0&*IB~#*2PwS4T+w6AJ5ru5GE3E4argD5lZ5A&ohIpXgqYpsf8BSZrqBUS_n5T",
        "x3IBHn_uw9LsGhOOz_QKlr59-z2CfZl=PcNr&EZQ0MB8J_vwgSO2CEHFflhN5mx^eZ0lr^hy+HltsMb++JgBvlgcm=oVS7~ALOJp",
        "xWZbxQy=TRRmtS3EpfduJ2xS1I8wIF3A@fMlOJyX^t+=eivHq5zLKtwqRT#BhDe#-8u-P7G5GCAm5ilAF~orIzT=^DhFcXVbEh4t",
        "xc+x2svuEJ3LY3TtU1b2AGN#Cp0bdYg*7Fdh&@mpFXath_J@9S0LXfM0Pmee9Ia-xZDsFnlZNE6tedmBcY_tgyHz~y&XqYEI+qKd",
        "xn&1+Sk61@F7i46*r*Zs3Vu+x1WyId7w8XZMNy6~4HWOMC=ZEWkyhG^=WY0MgoT1ITkSJkEDAB2lcapVhcJlSlPtiJ6+hoeWkN*@",
        "xsfy=SAnTbwXRGNhJu8h5#FwvGqMs0rv*FA8yWxgrieF+nJQ-W2zH7sWusWfCg5+C-JpFMaJ5fOgvQCw^VEgHe0KW2-_W6O-88K_",
        "y2rs6d6-k4vnTK*vVXm8_dHKa4BqJ8Y&~bz^EQp@d~ro84rQoDgJomVJqIG566oH_YDNn4~P0XpFTkb8k_wBMB=PLKCaWmWq#kp_",
        "y@K1Z27gg68bcpix+*L0hiZxCmipbRiFxkLu#-_MYNo@*mm1i@eixY-2TPZU@C8mlMeaoKi@G_ogftL^-STR93Qkp05f6wNgweMx",
        "yFYwQ1vAhqvVixC+~y-0yQi+ne3sltbBWGWy=&5^AU22GnRdQ0A&WW*4Fpw7R*Pdm63&hNfMZ*+qy0g^bg_&ZA7OezkQ0mPzQlol",
        "ySvya8^yQtwKYToOcMFQZ&B~azgFcUlyVYMyiHbdMb@Po0tH+ykn4fbQMb=Q6AYsxixTFsFm#0Mo-*&rFBEwHQ*Vmm8H*rK=BWX4",
        "yw_iO0Uzxcngrqg_IcNOsKfHXT8oFpmOzI1p*+S&a6g0f7qSFM1=nl9e8e@vTHHD~voMr^F*odcmTm=Sk1qrq5ULV7XFx-E&#NaQ",
        "z+I~+wZ6^=@8u4Gk^2~tE^+dv2Lx5&ucWy3PiZhVl84pDZ9ganLwUHM059UN2ehqy_*DUFcZKhtQ^79ht-xBFiSHb=Wy3zXVyg3v",
        "z4^35QfB-Nn79LnO0M5tpsgo6F@mBQe@#Ba^zgmZ2nq3-7XMpPg7BA0*^IJ&B-4VA@##1Sb5wz0yWf47RCIy=l9aOq#90q4AgT7W",
        "ziWkz4+r9CP4QT~=*O61mVBicuLWrZnrBXw*Hv5CRtd4&a3&=GeFg6Z@kOEdUr^QtxYWqIqnk=oUNk8e+e^U6ODJP1RHy5dlDp@H",
        "zw39a#N_HLIWM3smR=FeW_8X&RH3ro-@g2I9C=bbIxtpL8h&b7C#7+Df_0LWhO73XWtVruVgTWF9lx~y_FqPW-KWkFk78KtrfT8T",
        "~&P17@z0xgneiupBLL0#NvQmsH~&&52OJc=6vK3W^aNDLVIh3dSa+z6VWU65QL^&e9^7x08dg^Z@66KJ7M=mP^@SPmYJn~u95rr7",
        "~-ZA#eF^h76b+RrP_NDYDxp3*aBuwmM@*qelBs4+g-bvYh&+B=Gl4l+tR13FRecv7ghFz^+vMmlc#3AU&06Nm+__ph1F@lCcWWgF",
        "~1YCQ@=z+SfL_=fsAzZcv#JI9rLhyVN~tazbYuC3oi8QmpRFa5KuXefHyE4gECU#iOwwME1l8J~Y+BkI9z#Qk+-6_y*dbh_YksQ0",
        "~R_wsH~yk#HT79wwoQR*^IzPEW#P-sB^m9^t3UZ47dW0i5aizRZlFNU=nY=GKCy0IFIh0e0aAtU1Lu0EkImDo2xM9~7DcHNDA@xR",
        "~Ua5#2#o&641ztnx2yQTR~e0bOyknVQ1zhokO8GqFu&y^N4voPvWXu=lT8gxyZFYc&7upSQ=eaYWuqV33ecrECMwvJ4gnstxxw2S",
        "~hIMgd-_Tt=sVdSgUqEP4W@7dMgYqd9vFJLF4Z_aEF9Q0fb3P_hiBw87F6Fx5SrcnFuG2U=q3GbFQZN-BgGfSNG+=WGFD1Km&v-2",
        "~ykRZ9_I3_2mvl&yS8grf#r8hvf_vrUw9RlhA4EgTkYisn+xg#Ii=FYu8fNWuRkYCJGp4SF8_2MZnLkolBMOPog~SPk_Q2v*Os7*",
    ];

    #[traced_test]
    #[tokio::test]
    async fn test_run_with_fixtures() {
        // Initialize engine.
        let stor = mem::new();
        let wal_stor = MemStorage::init(InitialState::Blank).unwrap();
        let (req_tx, req_rx) = mpsc::channel(64);
        let engine = Engine::init(req_rx, wal_stor).unwrap();

        tokio::spawn(async move {
            if let Err(e) = engine.run(stor).await {
                panic!("engine exited with error: {:?}", e);
            };
        });

        for str in DATA {
            let key = Bytes::from(str);
            let value = Bytes::from(str);

            assert!(req_tx
                .send(Command::Set {
                    key,
                    value,
                    responder: None
                })
                .await
                .is_ok());
        }

        for str in DATA {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Get {
                key: Bytes::from(str),
                responder: resp_tx,
            };

            assert!(req_tx.send(cmd).await.is_ok());

            let resp = resp_rx.await;
            assert!(resp.is_ok(), "could not read response from channel");
            let resp = resp.unwrap();
            assert!(resp.is_ok(), "engine returned an error: {:?}", resp);

            let resp = resp.unwrap();
            assert!(resp.is_some(), "response have to be some but none returned");
            let resp = resp.unwrap();
            assert_eq!(resp, Bytes::from(str));
        }

        // Try to get value that is not present and should be none.
        let (resp_tx, resp_rx) = oneshot::channel();

        let missing_key = Bytes::from("example-key-that-was-never-set");
        let cmd = Command::Get {
            key: missing_key.clone(),
            responder: resp_tx,
        };

        assert!(req_tx.send(cmd).await.is_ok());

        let resp = resp_rx.await;
        assert!(resp.is_ok(), "could not read response from channel");
        let resp = resp.unwrap();
        assert!(resp.is_ok(), "engine returned an error: {:?}", resp);

        let resp = resp.unwrap();
        assert!(
            resp.is_none(),
            "key {:?} should be none but {:?} been returned",
            missing_key,
            resp
        );

        // TODO: Remake mem WAL storage to be wrapped in Arc to be able to get its state
        // before shutdown and test recover records from WAL.
        //
        // Shutdown engine.
        // let stored_data = wal_stor.logs();
        // let (engine_shutdown_rx, engine_shutdown_tx) = oneshot::channel();
        // assert!(req_tx
        //     .send(Command::Shutdown {
        //         responder: engine_shutdown_rx,
        //     })
        //     .await
        //     .is_ok());

        // assert!(engine_shutdown_tx.await.is_ok());

        // // Start engine again to check if state will be restored.
        // let (req_tx, req_rx) = mpsc::channel(64);
        // let engine = Engine::new(req_rx, wal_stor).unwrap();
        // tokio::spawn(async move {
        //     if let Err(e) = engine.run(stor).await {
        //         panic!("engine exited with error: {:?}", e);
        //     };
        // });
    }

    #[traced_test]
    #[tokio::test]
    async fn test_run_random_generated() {
        // Initialize engine.
        let stor = mem::new();
        let wal_stor = MemStorage::init(InitialState::Blank).unwrap();
        let (req_tx, req_rx) = mpsc::channel(64);
        let engine = Engine::init(req_rx, wal_stor).unwrap();
        tokio::spawn(async move {
            if let Err(e) = engine.run(stor).await {
                panic!("engine exited with error: {:?}", e);
            };
        });

        // Generate and populate entries.
        let entries_cnt = 2000;
        let mut entries: Vec<(Bytes, Bytes)> = vec![];
        for _ in 0..entries_cnt {
            let key = generate_valid_key();
            let value = generate_valid_value();

            entries.push((key.clone(), value.clone()));

            assert!(req_tx
                .send(Command::Set {
                    key,
                    value,
                    responder: None
                })
                .await
                .is_ok());
        }

        // Read values and record Nones if any.
        let mut nones: Vec<Bytes> = vec![];
        for entry in entries.clone() {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Get {
                key: entry.0.clone(),
                responder: resp_tx,
            };

            assert!(req_tx.send(cmd).await.is_ok());

            let resp = resp_rx.await;
            assert!(resp.is_ok(), "could not read response from channel");
            let resp = resp.unwrap();
            assert!(resp.is_ok(), "engine returned an error: {:?}", resp);

            let resp = resp.unwrap();
            if resp.is_none() {
                nones.push(entry.1.clone());
            }
        }

        // DEBUG
        if !nones.is_empty() {
            debug!("all keys: {:?}", entries);
            debug!("missing keys: {:?}", nones);
        }
        // END DEBUG

        assert!(
            nones.is_empty(),
            "there are {} values missing out of {}",
            nones.len(),
            entries.len(),
        );

        let (engine_shutdown_rx, engine_shutdown_tx) = oneshot::channel();
        req_tx
            .send(Command::Shutdown {
                responder: engine_shutdown_rx,
            })
            .await
            .unwrap();

        let _ = engine_shutdown_tx.await.unwrap();
    }

    fn generate_valid_key() -> Bytes {
        let mut rng = rng();
        let length = rng.random_range(1..=MAX_KEY_SIZE);
        let random_bytes: Vec<u8> = (0..length).map(|_| rng.random()).collect();
        Bytes::from(random_bytes)
    }

    fn generate_valid_value() -> Bytes {
        let mut rng = rng();
        let length = rng.random_range(1..=MAX_VALUE_SIZE);
        let random_bytes: Vec<u8> = (0..length).map(|_| rng.random()).collect();
        Bytes::from(random_bytes)
    }

    #[test]
    fn test_validate() {
        let long_arr: &'static [u8; 513] = &[0; 513];
        let longer_arr: &'static [u8; 2049] = &[0; 2049];
        let long_key = Bytes::from_static(long_arr);
        let long_value = Bytes::from_static(longer_arr);

        let res = validate(&long_key, &Bytes::from("asdf"));
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "key is too long");

        let res = validate(&Bytes::from("asdf"), &long_value);
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "value is too long");

        let res = validate(&Bytes::default(), &Bytes::from("asdf"));
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "key is empty");

        let res = validate(&Bytes::from("asdf"), &Bytes::default());
        assert!(res.is_err());
        assert_eq!(res.err().unwrap().to_string(), "value is empty");
    }
}
