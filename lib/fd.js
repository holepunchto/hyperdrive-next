const fs = require('fs')
const pump = require('pump')
const errors = require('./errors')
const messages = require('./messages')

const {
  O_RDONLY,
  O_WRONLY,
  O_RDWR,
  O_CREAT,
  O_TRUNC,
  O_APPEND,
  O_SYNC,
  O_EXCL
} = fs.constants
const O_ACCMODE = 3

class FileDescriptor {
  constructor (drive, path, stat, readable, writable, appending, creating) {
    this.drive = drive
    this.stat = stat
    this.path = path

    this.readable = readable
    this.writable = writable
    this.creating = creating
    this.appending = appending

    this.position = null
    this.blockPosition = stat ? stat.offset : null
    this.blockOffset = 0

    this._err = null
    if (this.writable) {
      this._writeStream = this.drive.createWriteStream(this.path)
      this._writeStream.on('error', err => {
        this._err = err
      })
      if (this.appending) this._appendStream = this.drive.createReadStream(this.path)
    }
  }

  read (buffer, offset, len, pos, cb) {
    if (!this.readable) return cb(new errors.BadFileDescriptor('File descriptor not open for reading.'))
    if (this.position !== null && this.position === pos) this._read(buffer, offset, len, cb)
    else this._seekAndRead(buffer, offset, len, pos, cb)
  }

  write (buffer, offset, len, pos, cb) {
    if (!this.writable) return cb(new errors.BadFileDescriptor('File descriptor not open for writing.'))
    if (!this.stat && !this.creating) {
      return cb(new errors.BadFileDescriptor('File descriptor not open in create mode.'))
    }
    if (this.position !== null && pos !== this.position) {
      return cb(new errors.BadFileDescriptor('Random-access writes are not currently supported.'))
    }
    if (this.appending && pos < this.stat.size) {
      return cb(new errors.BadFileDescriptor('Position cannot be less than the file size when appending.'))
    }
    if (!this._writeStream && !this.appending && pos !== 0) {
      return cb(new errors.BadFileDescriptor('Random-access writes are not currently supported.'))
    }

    const self = this

    // TODO: This is a temporary (bad) way of supporting appends.
    if (this._appendStream) {
      this.position = this.stat.size
      // pump does not support the `end` option.
      this._appendStream.pipe(this._writeStream, { end: false })

      this._appendStream.on('error', err => this._writeStream.destroy(err))
      this._writeStream.on('error', err => this._appendStream.destroy(err))

      return this._appendStream.on('end', doWrite)
    }

    return doWrite()

    function doWrite (err) {
      if (self._err) return cb(self._err)
      if (self._writeStream.destroyed) return cb(new errors.BadFileDescriptor('Write stream was destroyed.'))

      self._appendStream = null
      if (err) return cb(err)
      const slice = buffer.slice(offset, len)
      write(self._writeStream, slice, err => {
        if (err) return cb(err)
        self.position += slice.length
        return cb(null, slice.length, buffer)
      })
    }

    function write(stream, data, cb) {
      if (!stream.write(data)) {
        stream.once('drain', cb)
      } else {
        process.nextTick(cb)
      }
    }
  }

  close (cb) {
    // TODO: undownload initial range
    if (this._writeStream) {
      if (this._writeStream.destroyed) {
        this._writeStream = null
      } else {
        return this._writeStream.end(err => {
          if (err) return cb(err)  
          this._writeStream = null
          return cb(null)
        })
      }
    }
    process.nextTick(cb, null)
  }

  _seekAndRead (buffer, offset, len, pos, cb) {
    const start = this.stat.offset
    const end = start + this.stat.blocks

    this.drive.contentFeed.seek(this.stat.byteOffset + pos, { start, end }, (err, blk, blockOffset) => {
      if (err) return cb(err)
      this.position = pos
      this.blockPosition = blk
      this.blockOffset = blockOffset
      this._read(buffer, offset, len, cb)
    })
  }

  _read (buffer, offset, len, cb) {
    const buf = buffer.slice(offset, offset + len)
    const blkOffset = this.blockOffset
    const blk = this.blockPosition

    if ((this.stat.offset + this.stat.blocks) <= blk || blk < this.stat.offset) {
      return process.nextTick(cb, null, 0, buffer)
    }

    this.drive.contentFeed.get(blk, (err, data) => {
      if (err) return cb(err)
      if (blkOffset) data = data.slice(blkOffset)

      data.copy(buf)
      const read = Math.min(data.length, buf.length)

      if (blk === this.blockPosition && blkOffset === this.blockOffset) {
        this.position += read
        if (read === data.length) {
          this.blockPosition++
          this.blockOffset = 0
        } else {
          this.blockOffset = blkOffset + read
        }
      }

      cb(null, read, buffer)
    })
  }
}

FileDescriptor.create = function (drive, name, flags, cb) {
  try {
    flags = toFlagsNumber(flags)
  } catch (err) {
    return cb(err)
  }

  const accmode = flags & O_ACCMODE
  const writable = !!(accmode & (O_WRONLY | O_RDWR))
  const readable = accmode === 0 || !!(accmode & O_RDWR)
  const appending = !!(flags & O_APPEND)
  const creating = !!(flags & O_CREAT)
  const canExist = !(flags & O_EXCL)

  drive.contentReady(err => {
    if (err) return cb(err)
    drive._db.get(name, (err, st) => {
      if (err) return cb(err)
      if (st && !canExist) return cb(new errors.PathAlreadyExists(name))
      if (!st && (!writable || !creating)) return cb(new errors.FileNotFound(name))

      if (st) {
        try { 
          st = messages.Stat.decode(st.value)
        } catch (err) {
          return cb(err)
        }
      }

      cb(null, new FileDescriptor(drive, name, st, readable, writable, appending, creating))
    })
  })
}

module.exports = FileDescriptor

// Copied from the Node FS internal utils.
function toFlagsNumber (flags) {
  if (typeof flags === 'number') {
    return flags
  }

  switch (flags) {
    case 'r' : return O_RDONLY
    case 'rs' : // Fall through.
    case 'sr' : return O_RDONLY | O_SYNC
    case 'r+' : return O_RDWR
    case 'rs+' : // Fall through.
    case 'sr+' : return O_RDWR | O_SYNC

    case 'w' : return O_TRUNC | O_CREAT | O_WRONLY
    case 'wx' : // Fall through.
    case 'xw' : return O_TRUNC | O_CREAT | O_WRONLY | O_EXCL

    case 'w+' : return O_TRUNC | O_CREAT | O_RDWR
    case 'wx+': // Fall through.
    case 'xw+': return O_TRUNC | O_CREAT | O_RDWR | O_EXCL

    case 'a' : return O_APPEND | O_CREAT | O_WRONLY
    case 'ax' : // Fall through.
    case 'xa' : return O_APPEND | O_CREAT | O_WRONLY | O_EXCL
    case 'as' : // Fall through.
    case 'sa' : return O_APPEND | O_CREAT | O_WRONLY | O_SYNC

    case 'a+' : return O_APPEND | O_CREAT | O_RDWR
    case 'ax+': // Fall through.
    case 'xa+': return O_APPEND | O_CREAT | O_RDWR | O_EXCL
    case 'as+': // Fall through.
    case 'sa+': return O_APPEND | O_CREAT | O_RDWR | O_SYNC
  }

  throw new errors.InvalidArgument(`Invalid value in flags: ${flags}`)
}