from __future__ import print_function

path = "lists"

write_to_file = True

data = [["L133270", "L133272", "L133271", 0, 0],
        ["L136067", "L136069", "L136068", 0, 1],
        ["L136066", "L136064", "L136065", 0, 1],
        ["L138665", "L138663", "L138664", 0, 2],
        ["L138662", "L138660", "L138661", 0, 2],
        ["L138659", "L138657", "L138658", 0, 2],
        ["L139042", "L139040", "L139041", 0, 2],
        ["L228619", "L228623", "L228621", 2, 2],
        ["L228716", "L228720", "L228718", 2, 0],
        ["L229062", "L229066", "L229064", 2, 3],
        ["L229310", "L229314", "L229312", 2, 3],
        ["L229391", "L229389", "L229387", 2, 3],
        ["L229671", "L229675", "L229673", 2, 3],
        ["L230459", "L230463", "L230461", 2, 3],
        ["L230777", "L230781", "L230779", 2, 3],
        ["L231209", "L231213", "L231211", 2, 3],
        ["L231503", "L231507", "L231505", 2, 3],
        ["L231645", "L231649", "L231647", 2, 3],
        ["L232979", "L232983", "L232981", 2, 3],
        ["L233802", "L233806", "L233804", 2, 2],]
    

#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L138657/L138657_SAP000_SB000_uv.MS.tar
#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L138657/L138657_SAP000_SB370_uv.MS.tar
#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L136065_002/L136065_SAP006_SB484_uv.MS.dppp.tar
#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L136065_002/L136065_SAP006_SB484_uv.MS.dppp.tar

#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229064_002/L229064_SAP000_SB000_uv.MS.dppp.tar
#srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_229064_002/L229064_SAP002_SB420_uv.MS.dppp.tar

beams = [[ 69, 69, 69, 69, 69, 69, 69], 
         [371, 19, 19, 19, 19, 19, 19],    
         [371, 42, 15, 15, 15, 15, 15],    
         [371, 38, 38, 38]]

for i, d in enumerate(data):
    if write_to_file:
        f = open("{}/{:03d}_cycle{:d}_{}.list".format(path, i, d[3], d[2]), "wb")
    # Cycle 0
    if d[3] == 0:
        #Cal1
        for i in range(0, 371):
            line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/{L}/{L}_SAP000_SB{sb:03d}_uv.MS.tar".format(L=d[0], sb=i)
            print(line)
            if write_to_file:
                f.write(line+"\n")
        #Cal2
        for i in range(0, 371):
            line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/{L}/{L}_SAP000_SB{sb:03d}_uv.MS.tar".format(L=d[1], sb=i)
            print(line)
            if write_to_file:
                f.write(line+"\n")
        #Main
        n_init = 0
        for beam in range(len(beams[d[4]])):
            n = n_init + beams[d[4]][beam]
            for i in range(n_init, n):
                line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/{L}_002/{L}_SAP{beam:03d}_SB{sb:03d}_uv.MS.dppp.tar".format(L=d[2], beam=beam, sb=i)
                print(line)
                if write_to_file:
                    f.write(line+"\n")
            n_init = n
    elif d[3] == 2:
        #Cal1
        id_number = int(d[0][1:])
        for i in range(0, 371):
            line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_{Ln:d}/L{Ln:d}_SAP000_SB{sb:03d}_uv.MS.tar".format(Ln=id_number, sb=i)
            print(line)
            if write_to_file:
                    f.write(line+"\n")
        #Cal2
        id_number = int(d[1][1:])
        for i in range(0, 371):
            line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_{Ln:d}/L{Ln:d}_SAP000_SB{sb:03d}_uv.MS.tar".format(Ln=id_number, sb=i)
            print(line)
            if write_to_file:
                    f.write(line+"\n")
        #Main
        id_number = int(d[2][1:])
        n_init = 0
        for beam in range(len(beams[d[4]])):
            n = n_init + beams[d[4]][beam]
            for i in range(n_init, n):
                line = "srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lofar/ops/eor/L2014_{Ln:d}_002/L{Ln:d}_SAP{beam:03d}_SB{sb:03d}_uv.MS.dppp.tar".format(Ln=id_number, beam=beam, sb=i)
                print(line)
                if write_to_file:
                    f.write(line+"\n")
            n_init = n
    if write_to_file:
        f.close()

        
      
